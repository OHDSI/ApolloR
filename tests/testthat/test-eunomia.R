library(testthat)
library(ApolloR)
library(Eunomia)
library(FeatureExtraction)

connectionDetails <- getEunomiaConnectionDetails()
createCohorts(connectionDetails)
rootFolder <- tempfile("apollo_")
pretrainedModelFolder <- file.path(rootFolder, "pretrainedModel")
fineTunedModelFolder <- file.path(rootFolder, "fineTunedModel")


withr::defer({
  unlink(rootFolder, recursive = TRUE)
})

test_that("extractCdmToParquet on Eunomia", {
  cdmDatabaseSchema <- "main"
  workDatabaseSchema <- "main"
  
  extractCdmToParquet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    workDatabaseSchema = workDatabaseSchema,
    folder = rootFolder,
    sampleSize = 1000,
    partitions = 10,
    maxCores = 1,
    forceRestart = TRUE
  ) 
  personFiles <- list.files(file.path(rootFolder, "person"))
  expect_equal(length(personFiles), 10)
})


test_that("Custom covariate buider on Eunomia", {
  covariateSettings <- createCdmCovariateSettings(
    folder = rootFolder,
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
  covariateFolder <- attr(covariateData, "metaData")$parquetRootFolder
  subFolders <- list.dirs(covariateFolder, full.names = FALSE)
  expect_true("person" %in% subFolders)
  expect_false("concept" %in% subFolders)
  personFiles <- list.files(file.path(covariateFolder, "person"), full.names = FALSE)
  expect_equal(length(personFiles), 10)
})


test_that("Pretrain model on Eunomia", {
  # Skipping on GA for now, need to ensure Python installed and configured:
  skip_on_ci()
  reticulate::use_virtualenv("apollo")
  modelSettings <- createModelSettings(maxSequenceLength = 8,
                                       hiddenSize = 8,
                                       numAttentionHeads = 1,
                                       numHiddenLayers = 1,
                                       intermediateSize = 8)
  pretrainModel(parquetFolder = rootFolder,
                pretrainedModelFolder = pretrainedModelFolder,
                modelSettings = modelSettings,
                maxCores = 1)
  expect_true(file.exists(file.path(pretrainedModelFolder, "checkpoint_001.pth")))
})

test_that("Fine-tune on Eunomia", {
  # Skipping on GA for now, need to ensure Python installed and configured:
  skip_on_ci()
  reticulate::use_virtualenv("apollo")
  covariateSettings <- createCdmCovariateSettings(folder = rootFolder)
  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 3,
    covariateSettings = covariateSettings
  )
  rowIds <- covariateData$covariates %>%
    pull(.data$rowId)
  labels <- tibble(rowId = rowIds,
                   outcomeCount = rpois(length(rowIds), 0.2))
  trainingSettings <- createTrainingSettings(numEpochs = 1)
  fineTuneModel(pretrainedModelFolder = pretrainedModelFolder,
                fineTunedModelFolder = fineTunedModelFolder,
                covariateData = covariateData,
                labels = labels,
                trainingSettings = trainingSettings,
                modelType = "lstm",
                maxCores = 1)
  expect_true(file.exists(file.path(fineTunedModelFolder, "checkpoint_001.pth")))
})

test_that("Predict on Eunomia", {
  # Skipping on GA for now, need to ensure Python installed and configured:
  skip_on_ci()
  reticulate::use_virtualenv("apollo")
  covariateSettings <- createCdmCovariateSettings(folder = rootFolder)
  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 3,
    covariateSettings = covariateSettings
  )
  rowIds <- covariateData$covariates %>%
    pull(.data$rowId)
  population <- tibble(rowId = rowIds)
  prediction <- predictFineTuned(fineTunedModelFolder = fineTunedModelFolder,
                                 covariateData = covariateData,
                                 population = population,
                                 maxCores = 1)
  expect_true(all(population$rowId %in% prediction$observation_period_id))
})


# population <- population[order(population$rowId), ]
# prediction <- prediction[order(prediction$observation_period_id), ]


# R.utils::copyDirectory(fineTunedModelFolder, "~/data/DebugFineTuneModel/", recursive = TRUE)
# R.utils::copyDirectory(attr(covariateData, "metaData")$parquetRootFolder, "~/data/DebugCovariateData/", recursive = TRUE)
# population <- population %>%
#   mutate(outcomeCount = 0)
# ApolloR:::writeLabelsToParquet(labels = population,
#                                parquetRootFolder = attr(covariateData, "metaData")$parquetRootFolder)
# nrow(population)
# # [1] 479
