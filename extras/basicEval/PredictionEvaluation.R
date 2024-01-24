# Code for evaluating in the context of 3 patient-level prediction problems
library(dplyr)


targetId1 <- 301    # People aged 45-65 with a visit in 2013, no prior cancer
outcomeId1 <- 298   # Lung cancer
targetId2 <- 10460  # People aged 10- with major depressive disorder
outcomeId2 <- 10461 # Bipolar disorder
targetId3 <- 9938   # People aged 55=85 with a visit in 2012-2014, no prior dementia
outcomeId3 <- 6243  # Dementia

# CCAE
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_truven_ccae_v2633"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "apollo_test_cohorts"
rootFolder <- "D:/GPM_CCAE"
pretrainedModelFolder <- file.path(rootFolder, "model")

# Get cohort definitions -------------------------------------------------------
ROhdsiWebApi::authorizeWebApi(
  baseUrl = Sys.getenv("baseUrl"),
  authMethod = "windows")
cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  cohortIds = c(targetId1, outcomeId1, targetId2, outcomeId2, targetId3, outcomeId3),
  generateStats = TRUE,
  baseUrl =Sys.getenv("baseUrl")
)
saveRDS(cohortDefinitionSet, "extras/eval/cohortDefinitionSet.rds")

# Generate cohorts -------------------------------------------------------------
cohortDefinitionSet <- readRDS("extras/basicEval/cohortDefinitionSet.rds")
connection <- DatabaseConnector::connect(connectionDetails)
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable)
CohortGenerator::createCohortTables(
  connection = connection,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames
)
CohortGenerator::generateCohortSet(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet
)
DatabaseConnector::disconnect(connection)

#  Extract data ----------------------------------------------------------------
# Focusing on example 2 for now:
targetId <- targetId2
outcomeId <- outcomeId2
predictionFolder <- file.path(rootFolder, "pred2")
dir.create(predictionFolder)

# Get the target cohort and their labels (PLP normaly does this)
connection <- DatabaseConnector::connect(connectionDetails)
sql <- "
SELECT target_cohort.subject_id,
  target_cohort.cohort_start_date,
  CASE 
    WHEN outcome_cohort.subject_id IS NOT NULL THEN 1
    ELSE 0
  END AS has_outcome
FROM @schema.@table target_cohort
LEFT JOIN (
  SELECT subject_id,
    MIN(cohort_start_date) AS outcome_date
  FROM @schema.@table
  WHERE cohort_definition_id = @outcome_id
  GROUP BY subject_id
) outcome_cohort
  ON target_cohort.subject_id = outcome_cohort.subject_id
    AND target_cohort.cohort_start_date <= outcome_date
    AND DATEADD(DAY, 365, target_cohort.cohort_start_date) >= outcome_date
WHERE target_cohort.cohort_definition_id = @target_id;
"
population <- DatabaseConnector::renderTranslateQuerySql(
  connection = connection,
  sql = sql,
  schema = cohortDatabaseSchema,
  table = cohortTable,
  target_id = targetId,
  outcome_id = outcomeId,
  snakeCaseToCamelCase = TRUE)
saveRDS(population, file.path(predictionFolder, "FullPopulation.rds"))

# Select random sample and evenly split into train, test, and validation
sets <- population %>%
  mutate(rnd = runif(n())) %>%
  arrange(rnd) %>%
  mutate(rowNumber = row_number()) %>%
  filter(rowNumber <= 3000) %>%
  mutate(set = if_else(rowNumber <= 1000, "train", 
                       if_else(rowNumber <= 2000, "test",
                               "validation"))) %>%
  select(-rnd, -rowNumber)
saveRDS(sets, file.path(predictionFolder, "Sets.rds"))

# Extract covariates from database
data <- sets %>%
  select(subjectId, cohortStartDate) %>%
  mutate(cohortDefinitionId = 1)
DatabaseConnector::insertTable(
  connection = connection,
  tableName = "#cohort",
  data = data,
  dropTableIfExists = TRUE,
  createTable = TRUE,
  tempTable = TRUE,
  camelCaseToSnakeCase = TRUE)
covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(
  excludedCovariateConceptIds = 900000010
  )
covariateData <- FeatureExtraction::getDbCovariateData(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = "#cohort",
  cohortTableIsTemp = TRUE,
  covariateSettings = covariateSettings
)
FeatureExtraction::saveCovariateData(covariateData, file.path(predictionFolder, "AllDefaultCovs.zip"))
covariateSettings2 <- ApolloR::createCdmCovariateSettings(folder = file.path(predictionFolder, "CdmCovsFolder"))
covariateData2 <- FeatureExtraction::getDbCovariateData(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = "#cohort",
  cohortTableIsTemp = TRUE,
  covariateSettings = covariateSettings2
)
FeatureExtraction::saveCovariateData(covariateData2, file.path(predictionFolder, "CmdCovs.zip"))

DatabaseConnector::disconnect(connection)

# Train models -----------------------------------------------------------------
predictionFolder <- file.path(rootFolder, "pred2")
sets <- readRDS(file.path(predictionFolder, "Sets.rds"))

trainSet <- sets %>%
  filter(set == "train") %>%
  select(rowId = subjectId, outcomeCount = hasOutcome)


validationSet <- sets %>%
  filter(set == "validation") %>%
  select(rowId = subjectId, outcomeCount = hasOutcome)

# CEHR-BERT
covariateData2 <- FeatureExtraction::loadCovariateData(file.path(predictionFolder, "CmdCovs.zip"))
fineTunedModelFolder <- file.path(predictionFolder, "fineTuned")
trainSettings <- ApolloR::createTrainingSettings(trainFraction = 0.8,
                                                 numEpochs = 20)
                                                 
model <- ApolloR::fineTuneModel(
  pretrainedModelFolder = pretrainedModelFolder,
  fineTunedModelFolder = fineTunedModelFolder,
  covariateData = covariateData2,
  labels = trainSet,
  trainingSettings = trainSettings,
  maxCores = 4)


# LASSO
covariateData <- FeatureExtraction::loadCovariateData(file.path(predictionFolder, "AllDefaultCovs.zip"))
covariateData <- FeatureExtraction::filterByRowId(covariateData,  trainSet$rowId)
covariateData <- FeatureExtraction::tidyCovariateData(covariateData)
covariateData$outcomes <- trainSet %>%
  select(rowId, y = outcomeCount)
cyclopsData <- Cyclops::convertToCyclopsData(outcomes = covariateData$outcomes,
                                             covariates = covariateData$covariates,
                                             modelType = "lr")
fit <- Cyclops::fitCyclopsModel(cyclopsData, 
                                prior = Cyclops::createPrior("laplace", useCrossValidation = TRUE),
                                control = Cyclops::createControl(seed = 123,
                                                                 resetCoefficients = TRUE,
                                                                 noiseLevel = "quiet",
                                                                 fold = 10,
                                                                 threads = 10))
                                
covariateData <- FeatureExtraction::loadCovariateData(file.path(predictionFolder, "AllDefaultCovs.zip"))
covariateData <- FeatureExtraction::filterByRowId(covariateData,  validationSet$rowId)
covariateData <- FeatureExtraction::tidyCovariateData(covariateData)
covariateData$outcomes <- validationSet %>%
  select(rowId, y = outcomeCount)
prediction <- predict(fit, 
                      newOutcomes = covariateData$outcomes,
                      newCovariates = covariateData$covariates)

prediction <- tibble(rowId = as.numeric(names(prediction)),
                     value = prediction)
prediction <- prediction %>%
  inner_join(validationSet, by = join_by(rowId))
saveRDS(prediction, file.path(predictionFolder, "LassoPrediction.rds"))
rocObject <- pROC::roc(prediction$outcomeCount, prediction$value)
pROC::auc(rocObject)
# 0.7096

