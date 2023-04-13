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
# source("R/HelperFunctions.R")

#' Extract data from the database
#' 
#' @description 
#' Extract data from the server for a random sample of persons, and stores them 
#' in the local file system as Parquet files. Has the following features:
#' 
#' - Extracts the subset of CDM tables and fields listed here: https://github.com/OHDSI/GeneralPretrainedModelTools/blob/main/inst/tableColumnsToExtract.csv
#' - Can restrict to a sample of person_ids, as specified with the `sampleSize` argument.
#' - Loads and saves the tables in as many partitions as the user specifies (see `partitions` argument). The partitioning is done by person_id (or concept_id for the concept and concept_ancestor table), in a way that the n-th partition of each domain table refers to the same person_ids.
#' - Restricts the concept table to  standard concepts only (ie. those concepts that are allowed to be used in the CDM), to save space.
#' - Loading can be done with multiple threads (see `maxCores` argument) for speedup.
#' - If the process is interrupted for some reason (e.g. the server drops the connection) you can just restart it and it will pick up where it left off. (unless `forceRestart = TRUE`) .
#' 
#' @param connectionDetails    An R object of type `connectionDetails` created using the
#'                             [DatabaseConnector::createConnectionDetails()] function.
#' @param cdmDatabaseSchema    The name of the database schema that contains the OMOP CDM
#'                             instance. Requires read permissions to this database. On SQL
#'                             Server, this should specify both the database and the schema,
#'                             so for example 'cdm_instance.dbo'.
#' @param workDatabaseSchema   The name of the database schema where work tables can be created.
#' @param partitionTablePrefix The prefix to use when creating table names in the 
#'                             `workDatabaseSChema` for storing the person ID and concept ID 
#'                             partition tables.
#' @param folder               The folder on the local file system where the Parquet files will 
#'                             be written.
#' @param sampleSize           The number of persons to be included in the sample.
#' @param partitions           The number of partitions. Fewer partitions may lead to memory 
#'                             issues.
#' @param maxCores             The maximum number of parallel threads to use.
#' @param forceRestart         If `FALSE`, when data is already found in the `folder` the process
#'                             will continue where it left off. If `TRUE`, any existing data files
#'                             will be deleted, and the process will start from scratch.  
#' @param dropPartitionTablesWhenDone Drop the partition tables when done? If not, they could be 
#'                                    reused for a future data pull.
#'                                     
#' @export
extractCdmToParquet <- function(connectionDetails,
                                cdmDatabaseSchema,
                                workDatabaseSchema,
                                partitionTablePrefix = "GPM_",
                                folder,
                                sampleSize = 1e6,
                                partitions = 200,
                                maxCores = 3,
                                forceRestart = FALSE,
                                dropPartitionTablesWhenDone = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(workDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(partitionTablePrefix, len = 1, add = errorMessages)
  checkmate::assertCharacter(folder, len = 1, add = errorMessages)
  checkmate::assertInt(sampleSize, lower = 0, add = errorMessages)
  checkmate::assertInt(partitions, lower = 1, add = errorMessages)
  checkmate::assertInt(maxCores, lower = 1, add = errorMessages)
  checkmate::assertLogical(forceRestart, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  startTime <- Sys.time()
  
  ParallelLogger::addDefaultFileLogger(
    fileName = file.path(folder, "log.txt"),
    name = "DATA_EXTRACTION_FILE_LOGGER"
  )
  ParallelLogger::addDefaultErrorReportLogger(
    fileName = file.path(folder, "errorReportR.txt"),
    name = "DATA_EXRACTION_ERRORREPORT_LOGGER"
  )
  on.exit(ParallelLogger::unregisterLogger("DATA_EXTRACTION_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DATA_EXRACTION_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
  
  if (!dir.exists(folder)) {
    dir.create(folder, recursive = TRUE)
  }
  personIdPartitionTable <- sprintf("%s_pid_part", partitionTablePrefix)
  conceptIdPartitionTable <- sprintf("%s_cid_part", partitionTablePrefix)
  restart <- createPartitionTables(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    personIdPartitionTable = personIdPartitionTable,
    conceptIdPartitionTable = conceptIdPartitionTable,
    sampleSize = sampleSize,
    partitions = partitions,
    forceRestart = forceRestart
  )
  createFolders(folder, restart)
  jobs <- createJobList(partitions, folder)
  executeExtractDataJobs(
    jobs = jobs, 
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    personIdPartitionTable = personIdPartitionTable,
    conceptIdPartitionTable = conceptIdPartitionTable,
    maxCores = maxCores
  )
  if (dropPartitionTablesWhenDone) {
    dropPartitionTables(
      connectionDetails = connectionDetails,
      workDatabaseSchema = workDatabaseSchema,
      personIdPartitionTable = personIdPartitionTable,
      conceptIdPartitionTable = conceptIdPartitionTable
    )
  }
  
  delta <- Sys.time() - startTime
  message(paste("Extracting data took", signif(delta, 3), attr(delta, "units")))
}

createPartitionTables <- function(connectionDetails,
                                  cdmDatabaseSchema,
                                  workDatabaseSchema,
                                  personIdPartitionTable,
                                  conceptIdPartitionTable,
                                  sampleSize,
                                  partitions,
                                  forceRestart) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  if (!forceRestart && 
      DatabaseConnector::existsTable(
        connection = connection,
        databaseSchema = workDatabaseSchema,
        tableName = personIdPartitionTable
      ) &&
      DatabaseConnector::existsTable(
        connection = connection,
        databaseSchema = workDatabaseSchema,
        tableName = conceptIdPartitionTable
      )
  ) {
    message("Partition tables already exists, so using those")
    return(FALSE)
  } else {
    message("Creating partition tables")
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CreatePartitionTables.sql",
      packageName = "GeneralPretrainedModelTools",
      dbms = connectionDetails$dbms,
      cdm_database_schema = cdmDatabaseSchema,
      work_database_schema = workDatabaseSchema,
      person_id_partition_table = personIdPartitionTable,
      concept_id_partition_table = conceptIdPartitionTable,
      sample_size = sampleSize,
      partitions = partitions
    )
    DatabaseConnector::executeSql(connection, sql, reportOverallTime = FALSE)
    return(TRUE)
  }
}

createJobList <- function(partitions, folder) {
  tablesToExtract <- getTableColumnsToExtract() %>%
    distinct(.data$cdmTableName) %>%
    pull(.data$cdmTableName)
  jobs <- expand.grid(table = tablesToExtract, partition = seq_len(partitions)) %>%
    as_tibble() %>%
    mutate(fileName = file.path(folder, .data$table, sprintf("part%04d.parquet", .data$partition)))
  return(jobs)
}

createFolders <- function(folder, restart) {
  foldersToCreate <- getTableColumnsToExtract() %>%
    distinct(.data$cdmTableName) %>%
    mutate(folder = file.path(!!folder, .data$cdmTableName)) %>%
    pull(.data$folder)
  if (restart) {
    for (folder in foldersToCreate) {
      if (dir.exists(folder)) {
        message(sprintf("Folder %s already exists. Deleting old data", folder))
        unlink(folder, recursive = TRUE)
      }
    }
  }
  for (folder in foldersToCreate) {
    if (!dir.exists(folder)) {
      dir.create(folder, recursive = TRUE)
    }
  }
}

executeExtractDataJobs <- function(jobs, 
                                   connectionDetails,
                                   cdmDatabaseSchema,
                                   workDatabaseSchema,
                                   personIdPartitionTable,
                                   conceptIdPartitionTable,
                                   maxCores) {
  jobs <- jobs %>%
    filter(!file.exists(.data$fileName)) %>%
    mutate(sleepSeconds = 0)
  if (nrow(jobs) == 0) {
    return()
  }
  # Stagger jobs so as not to overwhelm database server at start.
  # (Not sure if this helps, but seems a good idea)
  for (i in seq_len(min(maxCores, nrow(jobs)))) {
    jobs$sleepSeconds[i] <- (i - 1) * 30
  }
  clusterSize <- min(maxCores, nrow(jobs))
  cluster <- ParallelLogger::makeCluster(clusterSize)
  on.exit(ParallelLogger::stopCluster(cluster))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  message("Opening connections")
  ParallelLogger::clusterApply(
    cluster = cluster, 
    x = seq_len(clusterSize), 
    fun = createConnection,
    connectionDetails = connectionDetails
  )
  message("Extracting data from CDM to Parquet files")
  jobs <- split(jobs, seq_len(nrow(jobs)))
  ParallelLogger::clusterApply(
    cluster = cluster, 
    x = jobs, 
    fun = executeExtractDataJob,
    stopOnError = TRUE,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    personIdPartitionTable = personIdPartitionTable,
    conceptIdPartitionTable = conceptIdPartitionTable
  )
}

threadEnv <- new.env()

createConnection <- function(dummy, 
                             connectionDetails) {
  connection <- DatabaseConnector::connect(connectionDetails)
  assign("connection", connection, envir = threadEnv)
  reg.finalizer(threadEnv, closeConnection, onexit = TRUE)
}

closeConnection <- function(env) {
  if (exists("connection", envir = env)) {
    connection <- get("connection", envir = env)
    DatabaseConnector::disconnect(connection)
  }
}

executeExtractDataJob <- function(job, 
                                  cdmDatabaseSchema,
                                  workDatabaseSchema,
                                  personIdPartitionTable,
                                  conceptIdPartitionTable) {
  connection <- get("connection", envir = threadEnv)
  columnsToExtract <- getTableColumnsToExtract() %>%
    filter(.data$cdmTableName == job$table) %>%
    pull(cdmFieldName)
  
  sql <- sprintf("SELECT %s\nFROM %s.%s",
                 paste(paste(job$table, columnsToExtract, sep = "."), collapse = ",\n  "),
                 cdmDatabaseSchema,
                 job$table)
  if (job$table == "concept") {
    sql <- paste0(
      sql,
      sprintf("\nINNER JOIN %s.%s\n  ON concept.concept_id = %s.concept_id\nWHERE partition_id = %d;",
              workDatabaseSchema,
              conceptIdPartitionTable,
              conceptIdPartitionTable,
              job$partition
      )
    )
  } else if (job$table == "concept_ancestor") {
    sql <- paste0(
      sql,
      sprintf("\nINNER JOIN %s.%s\n  ON concept_ancestor.ancestor_concept_id = %s.concept_id\nWHERE partition_id = %d;",
              workDatabaseSchema,
              conceptIdPartitionTable,
              conceptIdPartitionTable,
              job$partition
      )
    )
  } else {
    sql <- paste0(
      sql,
      sprintf("\nINNER JOIN %s.%s\n  ON %s.person_id = %s.person_id\nWHERE partition_id = %d;",
              workDatabaseSchema,
              personIdPartitionTable,
              job$table,
              personIdPartitionTable,
              job$partition
      )
    )
  }
  if (job$sleepSeconds > 0) {
    message(sprintf("Waiting for %d seconds before sending query to server", job$sleepSeconds))
    Sys.sleep(job$sleepSeconds)
  }
  message(sprintf("Fetching partition %d of table %s", job$partition, job$table))
  data <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    integer64AsNumeric = FALSE
  )
  message(sprintf("Writing data partition to %s", job$fileName))
  arrow::write_parquet(
    x = data,
    sink = job$fileName,
  )
}

dropPartitionTables <- function(connectionDetails,
                                workDatabaseSchema,
                                personIdPartitionTable,
                                conceptIdPartitionTable) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  message("Dropping partition tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropPartitionTables.sql",
    packageName = "GeneralPretrainedModelTools",
    dbms = connectionDetails$dbms,
    work_database_schema = workDatabaseSchema,
    person_id_partition_table = personIdPartitionTable,
    concept_id_partition_table = conceptIdPartitionTable
  )
  DatabaseConnector::executeSql(connection, sql, reportOverallTime = FALSE)
}
