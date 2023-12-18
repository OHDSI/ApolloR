library(testthat)
library(GeneralPretrainedModelTools)
library(Eunomia)
library(FeatureExtraction)

connectionDetails <- getEunomiaConnectionDetails()
createCohorts(connectionDetails)

test_that("extractCdmToParquet on Eunomia", {
  cdmDatabaseSchema <- "main"
  workDatabaseSchema <- "main"
  folder <- tempfile("gpmt_")
  on.exit(unlink(folder, recursive = TRUE))
  
  extractCdmToParquet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    folder = folder,
    sampleSize = 1000,
    partitions = 10,
    maxCores = 1,
    forceRestart = TRUE
  ) 
  personFiles <- list.files(file.path(folder, "person"))
  expect_equal(length(personFiles), 10)
})


test_that("extractCdmToParquet on Eunomia", {
  folder <- tempfile("gpmt_")
  on.exit(unlink(folder, recursive = TRUE))
  covariateSettings <- createCdmCovariateSettings(
    folder = folder,
    windowStart = -365,
    windowEnd = 0,
    partitions = 10,
    analysisId = 999
  )
  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 3,
    covariateSettings = covariateSettings
  )
  rootFolder <- attr(covariateData, "metaData")$parquetRootFolder
  subFolders <- list.dirs(rootFolder, full.names = FALSE)
  expect_true("person" %in% subFolders)
  expect_false("concept" %in% subFolders)
  personFiles <- list.files(file.path(rootFolder, "person"), full.names = FALSE)
  expect_equal(length(personFiles), 10)
})
