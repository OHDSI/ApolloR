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
predictionFolder <- file.path(rootFolder, "pred2")
dir.create(predictionFolder)
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
  target_id = targetId2,
  outcome_id = outcomeId2,
  snakeCaseToCamelCase = TRUE)
saveRDS(population, file.path(predictionFolder, "FullPopulation.rds"))

sets <- population %>%
  mutate(rnd = runif(n())) %>%
  arrange(rnd) %>%
  mutate(rowNumber = row_number()) %>%
  filter(rowNumber <= 300000) %>%
  mutate(set = if_else(rowNumber <= 100000, "train", 
                       if_else(rowNumber <= 200000, "test",
                               "validation"))) %>%
  select(-rnd, -rowNumber)


saveRDS(sets, file.path(predictionFolder, "Sets.rds"))

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


    
    
    
    
    
    

restrictPlpDataSettings1 <- PatientLevelPrediction::createRestrictPlpDataSettings(
  sampleSize = 100000
)
restrictPlpDataSettings2 <- PatientLevelPrediction::createRestrictPlpDataSettings(
  sampleSize = 10000
)
populationSettings1 = PatientLevelPrediction::createStudyPopulationSettings(
  washoutPeriod = 365,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 1095,
  endAnchor = "cohort start",
  removeSubjectsWithPriorOutcome = TRUE,
  priorOutcomeLookback = 999999,
  requireTimeAtRisk = FALSE
)
populationSettings2 = PatientLevelPrediction::createStudyPopulationSettings(
  washoutPeriod = 365,
  riskWindowStart = 2,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  removeSubjectsWithPriorOutcome = TRUE,
  priorOutcomeLookback = 999999,
  requireTimeAtRisk = FALSE
)
demographicsCovariateSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAgeGroup = TRUE,
  useDemographicsRace = TRUE,
  useDemographicsEthnicity = TRUE
)
baseCovariateSettings <- GloVeHd::createBaseCovariateSettings(type = "binary")
conceptVectors <- readRDS(file.path(folder, "ConceptVectors.rds"))
gloVeCovariateSettings <- GloVeHd::createGloVeCovariateSettings(
  baseCovariateSettings = baseCovariateSettings,
  conceptVectors = conceptVectors
)
defaultCovariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
defaultCovariateSettings$DemographicsIndexYear <- FALSE
defaultCovariateSettings$DemographicsIndexMonth <- FALSE
modelSettings <- PatientLevelPrediction::setLassoLogisticRegression()
modelDesign1 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId1,
  outcomeId = outcomeId1,
  populationSettings = populationSettings1,
  restrictPlpDataSettings = restrictPlpDataSettings1,
  covariateSettings = list(baseCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign2 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId1,
  outcomeId = outcomeId1,
  populationSettings = populationSettings1,
  restrictPlpDataSettings = restrictPlpDataSettings1,
  covariateSettings = list(gloVeCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(normalize = FALSE),
  modelSettings = modelSettings
)
modelDesign3 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId1,
  outcomeId = outcomeId1,
  populationSettings = populationSettings1,
  restrictPlpDataSettings = restrictPlpDataSettings1,
  covariateSettings = defaultCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign4 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId1,
  outcomeId = outcomeId1,
  populationSettings = populationSettings1,
  restrictPlpDataSettings = restrictPlpDataSettings1,
  covariateSettings = demographicsCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign5 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId2,
  outcomeId = outcomeId2,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = list(baseCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign6 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId2,
  outcomeId = outcomeId2,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = list(gloVeCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(normalize = FALSE),
  modelSettings = modelSettings
)
modelDesign7 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId2,
  outcomeId = outcomeId2,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = defaultCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign8 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId2,
  outcomeId = outcomeId2,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = demographicsCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign9 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId3,
  outcomeId = outcomeId3,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = list(baseCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign10 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId3,
  outcomeId = outcomeId3,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = list(gloVeCovariateSettings, demographicsCovariateSettings),
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(normalize = FALSE),
  modelSettings = modelSettings
)
modelDesign11 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId3,
  outcomeId = outcomeId3,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = defaultCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)
modelDesign12 <- PatientLevelPrediction::createModelDesign(
  targetId = targetId3,
  outcomeId = outcomeId3,
  populationSettings = populationSettings2,
  restrictPlpDataSettings = restrictPlpDataSettings2,
  covariateSettings = demographicsCovariateSettings,
  preprocessSettings = PatientLevelPrediction::createPreprocessSettings(),
  modelSettings = modelSettings
)

modelDesignList <- list(
  modelDesign1, 
  modelDesign2, 
  modelDesign3, 
  modelDesign4, 
  modelDesign5, 
  modelDesign6, 
  modelDesign7, 
  modelDesign8, 
  modelDesign9,
  modelDesign10, 
  modelDesign11, 
  modelDesign12
)
databaseDetails <- PatientLevelPrediction::createDatabaseDetails(
  connectionDetails = connectionDetails, 
  cdmDatabaseSchema = cdmDatabaseSchema, 
  cdmDatabaseName = cdmDatabaseSchema, 
  cdmDatabaseId = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema, 
  cohortTable = cohortTable, 
  outcomeDatabaseSchema = cohortDatabaseSchema, 
  outcomeTable = cohortTable
)
ParallelLogger::addDefaultFileLogger(
  fileName = file.path(folder, "Plp", "log.txt"), 
  name = "PLPLOG"
)
results <- PatientLevelPrediction::runMultiplePlp(
  databaseDetails = databaseDetails, 
  modelDesignList = modelDesignList,
  saveDirectory = file.path(folder, "Plp")
)
ParallelLogger::unregisterLogger("PLPLOG")

# View results ----------------------------------------------------------------
PatientLevelPrediction::viewMultiplePlp(file.path(folder, "Plp"))

library(dplyr)
getStats <- function(analysisId) {
  runPlp <- readRDS(file.path(folder, "Plp", sprintf("Analysis_%d", analysisId), "plpResult", "runPlp.rds"))
  return(tibble(
    trainPopulationSize = as.numeric(runPlp$performanceEvaluation$evaluationStatistics$value[[20]]),
    trainOutcomeCount = as.numeric(runPlp$performanceEvaluation$evaluationStatistics$value[[21]]),
    testAUC = as.numeric(runPlp$performanceEvaluation$evaluationStatistics$value[[3]]),
    testBrierScore = as.numeric(runPlp$performanceEvaluation$evaluationStatistics$value[[7]])
  ))
}


results <- tibble(
  outcome = rep(c("Lung cancer", "Bipolar disorder", "Dementia"), each = 4),
  covariates = rep(c("Verbatim concepts + demographics", "GloVe + demographics", "FeatureExtraction default", "Demographics"), 3)
)

stats <- lapply(1:12, getStats)
stats <- bind_rows(stats)
results <- bind_cols(results, stats)
readr::write_csv(results, file.path(folder, "Results.csv"))

baseCovariateData <- FeatureExtraction::loadCovariateData(file.path(folder, "Plp", "targetId_301_L1", "covariates"))
covariateData <- FeatureExtraction::loadCovariateData(file.path(folder, "Plp", "targetId_301_L2", "covariates"))



sum(as.numeric(rownames(conceptVectors)) == 900000010)
