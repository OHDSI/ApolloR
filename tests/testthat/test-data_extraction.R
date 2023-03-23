library(testthat)
library(GeneralPretrainedModelTools)

# Note: very slow becuase we always extract the full concept table:

# Postgres ----------------------------------------------------------
test_that("extractCdmToParquet on Postgres", {
  
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
  )
  cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  workDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
  folder <- tempfile("gpmt_")
  on.exit(unlink(folder, recursive = TRUE))
  
  extractCdmToParquet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    partitionTablePrefix = SqlRender::getTempTablePrefix(),
    folder = folder,
    sampleSize = 1000,
    partitions = 10,
    maxCores = 2,
    forceRestart = FALSE
  ) 
  personFiles <- list.files(file.path(folder, "person"))
  expect_equal(length(personFiles), 10)
})

# SQL Server --------------------------------------
   
# Oracle ---------------------------------------
