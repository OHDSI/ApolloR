# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of GeneralPretrainedModelTools
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

#' Extract data from the database
#' 
#' @description 
#' Extract data from the server for a random sample of persons, and stores them 
#' in the local file system as Parquert files
#' 
#' 
#' @param connectionDetails            An R object of type `connectionDetails` created using the
#'                                     [DatabaseConnector::createConnectionDetails()] function.
#' @param cdmDatabaseSchema            The name of the database schema that contains the OMOP CDM
#'                                     instance. Requires read permissions to this database. On SQL
#'                                     Server, this should specify both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param workDatabaseSchema           The name of the database schema where work tables can be created.
#' @param sampleTable                  The name of the table where the sampled observation period IDs 
#'                                     will be stored.
#' @param folder                       The folder on the local file system where the Parquet files will 
#'                                     be written.
#' @param sampleSize                   The number of persons to be included in the sample.
#' @param partitions                   The number of partitions. Fewer partitions may lead to memory 
#'                                     issues.
#' @param maxCores                     The maximum number of parallel threads to use.
#'                                     
#' @export
extractData <- function(connectionDetails,
                        cdmDatabaseSchema,
                        workDatabaseSchema,
                        sampleTable = "GeneralPretrainedModelTools_sample",
                        folder,
                        sampleSize = 1e6,
                        partitions = 200,
                        maxCores = 3) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(workDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(sampleTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(folder, len = 1, add = errorMessages)
  checkmate::assertInt(sampleSize, lower = 0, add = errorMessages)
  checkmate::assertInt(partitions, lower = 1, add = errorMessages)
  checkmate::assertInt(maxCores, lower = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  DatabaseConnector::assertTempEmulationSchemaSet(connectionDetails$dbms)
  
  startTime <- Sys.time()
  
  createSampleTable(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    sampleTable = sampleTable,
    sampleSize = sampleSize,
    partitions = partitions
  )
  
  tableColumnsToExtract <- getTableColumnsToExtract()
  domainTables <- tableColumnsToExtract %>%
    filter(!grepl("^concept", .data$cdmTableName)) %>%
    distinct(.data$cdmTableName) %>%
    pull(.data$cdmTableName)
  
  conceptTables <- tableColumnsToExtract %>%
    filter(grepl("^concept", .data$cdmTableName)) %>%
    distinct(.data$cdmTableName) %>%
    pull(.data$cdmTableName)
  
  for (table in c(domainTables, conceptTables)) {
    dir.create(file.path(folder, table), recursive = TRUE)
  }
  
  jobs <- expand.grid(table = domainTables, partition = seq_len(partitions)) %>%
    bind_rows(tibble(table = conceptTables, partition = NA)) 
  jobs <- split(jobs, seq_len(nrow(jobs)))
  
  cluster <- ParallelLogger::makeCluster(maxCores)
  on.exit(ParallelLogger::stopCluster(cluster))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  ParallelLogger::clusterApply(
    cluster = cluster, 
    x = seq_len(maxCores), 
    fun = createConnection,
    connectionDetails = connectionDetails
  )
  ParallelLogger::clusterApply(
    cluster = cluster, 
    x = jobs, 
    fun = executeExtractDataJob,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    sampleTable = sampleTable,
    folder = folder
  )
  delta <- Sys.time() - startTime
  message(paste("Extracting data took", signif(delta, 3), attr(delta, "units")))
  
}

createSampleTable <- function(connectionDetails,
                              cdmDatabaseSchema,
                              workDatabaseSchema,
                              sampleTable,
                              sampleSize,
                              partitions) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  message("Taking sample")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateSample.sql",
    packageName = "GeneralPretrainedModelTools",
    dbms = connectionDetails$dbms,
    cdm_database_schema = cdmDatabaseSchema,
    work_database_schema = workDatabaseSchema,
    sample_table = sampleTable,
    sample_size = sampleSize,
    partitions = partitions
  )
  DatabaseConnector::executeSql(connection, sql, reportOverallTime = FALSE)
}

threadEnv <- new.env()

createConnection <- function(job, 
                             connectionDetails) {
  connection <- DatabaseConnector::connect(connectionDetails)
  assign("connection", connection, envir = threadEnv)
}

.Last <- function() {
  if (exists("connection", envir = threadEnv)) {
    connection <- get("connection", envir = threadEnv)
    remove("connection", envir = threadEnv)
    ParallelLogger::logDebug("Disconnecting from server")
    DatabaseConnector::disconnect(connection)
  }
}

executeExtractDataJob <- function(job, 
                                  cdmDatabaseSchema,
                                  workDatabaseSchema,
                                  sampleTable = sampleTable,
                                  folder) {
  connection <- get("connection", envir = threadEnv)
  columnsToExtract <- getTableColumnsToExtract() %>%
    filter(.data$cdmTableName == job$table) %>%
    pull(cdmFieldName)
  
  sql <- sprintf("SELECT %s\nFROM %s.%s",
                 paste(paste(job$table, columnsToExtract, sep = "."), collapse = ",\n  "),
                 cdmDatabaseSchema,
                 job$table)
  if (is.na(job$partition)) {
    if (job$table == "concept") {
      sql <- paste0(sql, "\nWHERE standard_concept = 'S';")
    }
    sql <- paste0(sql, ";")
  } else {
    sql <- paste0(sql,
                  sprintf("\nINNER JOIN %s.%s\n  ON %s.person_id = %s.person_id\nWHERE partition_id = %d;",
                          workDatabaseSchema,
                          sampleTable,
                          job$table,
                          sampleTable,
                          job$partition
                  )
    )
  }
  data <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql
  )
  if (is.na(job$partition)) {
    fileName <- "all.parquet"
  } else {
    fileName <- sprintf("part%04d.parquet", job$partition)
  }
  
  arrow::write_parquet(
    x = data,
    sink = file.path(folder, job$table, fileName),
  )
}
