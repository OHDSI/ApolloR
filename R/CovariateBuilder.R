# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of ApolloR
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# library(dplyr)
# source("R/HelperFunctions.R")

#' Create CDM covariate settings 
#' 
#' @description
#' Create covariate settings for extracting verbatim data from a subset of fields 
#' of a subset of tables in the OMOP Common Data Model 
#' 
#' @param folder      The folder on the local file system where the Parquet files 
#'                    will be written.
#' @param windowStart The start of the window relative to the cohort start date 
#'                    where CDM data will be extracted.
#' @param windowEnd   The end of the window relative to the cohort start date 
#'                    where CDM data will be extracted.
#' @param partitions  The number of partitions to divide the data in.
#' @param analysisId  The covariate analysis ID. 
#'
#' @return 
#' An object of type `covariateSettings`, to be used with the `FeatureExtraction` 
#' package.
#' 
#' @export
createCdmCovariateSettings <- function(folder, 
                                       windowStart = -365, 
                                       windowEnd = 0,
                                       partitions = 10,
                                       analysisId = 999) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(folder, len = 1, add = errorMessages)
  checkmate::assertInt(windowStart, upper = 0, add = errorMessages)
  checkmate::assertInt(windowEnd, upper = 0, add = errorMessages)
  checkmate::assertInt(partitions, lower = 1, add = errorMessages)
  checkmate::assertInt(analysisId, lower = 0, upper = 999, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  covariateSettings <- list(folder = folder,
                            windowStart = windowStart,
                            windowEnd = windowEnd,
                            partitions = partitions,
                            analysisId = analysisId)
  attr(covariateSettings, "fun") <- "ApolloR:::getDbCdmCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

getDbCdmCovariateData <- function(connection,
                                  oracleTempSchema = NULL,
                                  cdmDatabaseSchema,
                                  cdmVersion = "5",
                                  cohortTable = "#cohort_person",
                                  cohortId = -1,
                                  rowIdField = "subject_id",
                                  covariateSettings,
                                  aggregated = FALSE) {
  if (aggregated) {
    stop("Aggregation not supported")
  }
  
  # Data is not compatible with FeatureExtraction's CovariateData class. So we
  # store a dummy covariate per cohort entry so we'll know when covariates
  # have been subseted to a specific population. We add metaData that points 
  # to the folder where the Parquet files live.
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateCohortPartitionTable.sql",
    packageName = "ApolloR",
    dbms = DatabaseConnector::dbms(connection),
    cdm_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    row_id_field = rowIdField,
    window_start = covariateSettings$windowStart,
    window_end = covariateSettings$windowEnd,
    partitions = covariateSettings$partitions
  )
  DatabaseConnector::executeSql(connection = connection, 
                                sql = sql, 
                                progressBar = FALSE, 
                                reportOverallTime = FALSE)
  subFolder <- file.path(covariateSettings$folder,
                         sprintf("c_%s_%s", 
                                 cohortId, 
                                 paste(sample(letters, 10), collapse = "")))
  extractCdmTablesForCohort(connection = connection, 
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            subFolder = subFolder,
                            partitions = covariateSettings$partitions)
  
  sql <- "SELECT observation_period_id AS row_id FROM #partition_table;"
  cohortEntries <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  )
  sql <- "TRUNCATE TABLE #partition_table; DROP TABLE #partition_table;"
  DatabaseConnector::renderTranslateExecuteSql(connection, sql)
  
  # Construct dummy covariates:
  covariates <- cohortEntries %>%
    mutate(covariateId = 1000 + covariateSettings$analysisId,
           covariateValue = 1) %>%
    select("rowId", "covariateId", "covariateValue")
  
  # Construct covariate reference:
  covariateRef <- data.frame(
    covariateId = 1,
    covariateName = "Cohort entry marker for ApolloR",
    analysisId = covariateSettings$analysisId,
    conceptId = 0
  )
  
  # Construct analysis reference:
  analysisRef <- data.frame(
    analysisId = covariateSettings$analysisId,
    analysisName = "Cohort entry marker for ApolloR",
    domainId = "All",
    startDay = 0,
    endDay = 0,
    isBinary = "N",
    missingMeansZero = "Y"
  )
  
  # Create CovariateData object
  metaData <- list(parquetRootFolder = subFolder)
  result <- Andromeda::andromeda(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef
  )
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"
  return(result)
}

extractCdmTablesForCohort <- function(connection, cdmDatabaseSchema, subFolder, partitions) {
  createFolders(folder = subFolder, restart = FALSE, skipConceptTables = TRUE)
  jobs <- createJobList(partitions = partitions,
                           folder = subFolder,
                           skipConceptTables = TRUE)
  jobs <- split(jobs, seq_len(nrow(jobs)))
  invisible(lapply(jobs, executeExtractCovariateDataJob, connection = connection, cdmDatabaseSchema = cdmDatabaseSchema))
}

executeExtractCovariateDataJob <- function(job,
                                           connection,
                                           cdmDatabaseSchema) {
  columnsToExtract <- getTableColumnsToExtract() %>%
    filter(.data$cdmTableName == job$table) %>%
    pull(.data$cdmFieldName)
  fields <- paste(paste(job$table, columnsToExtract, sep = "."), collapse = ",\n  ")
  if (job$table == "observation_period") {
    sql <- sprintf("SELECT %s\nFROM #partition_table observation_period;",
                   paste(paste(job$table, columnsToExtract, sep = "."), collapse = ",\n  "))
    sql <- SqlRender::translate(sql, DatabaseConnector::dbms(connection))
  } else {
    dateField <- getTableColumnsToExtract() %>%
      filter(.data$cdmTableName == job$table, .data$startDate == "yes") %>%
      pull(.data$cdmFieldName)
    
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "GetCovariateData.sql",
      packageName = "ApolloR",
      dbms = DatabaseConnector::dbms(connection),
      cdm_database_schema = cdmDatabaseSchema,
      cdm_table = job$table,
      fields = fields,
      start_date_field = dateField,
      partition_id = job$partition
    )
  }
  # message(sprintf("Fetching partition %d of table %s", job$partition, job$table))
  data <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    integer64AsNumeric = FALSE,
    integerAsNumeric = FALSE
  )
  colnames(data) <- tolower(colnames(data))
  data <- enforceDataTypes(data, job$table)
  # message(sprintf("Writing data partition to %s", job$fileName))
  arrow::write_parquet(
    x = data,
    sink = job$fileName,
  )
}
