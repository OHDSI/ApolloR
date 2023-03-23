library(GeneralPretrainedModelTools)

maxCores <- max(8, parallel::detectCores())

# Settings ---------------------------------------------------------------------

# MDCD
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_truven_mdcd_v2321"
workDatabaseSchema <- "scratch_mschuemi"
partitionTablePrefix <- "GPM_MDCD"
folder <- "d:/GPM_MDCD"

# Optum EHR
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaOptumEhr"),
  user = keyring::key_get("temp_user"),
  password = keyring::key_get("temp_password")
)
cdmDatabaseSchema <- "cdm_optum_ehr_v2247"
workDatabaseSchema <- "scratch_mschuemi"
sampleTable <- "GeneralPretrainedModelTools_optum_ehr"
folder <- "d:/GeneralPretrainedModelTools_OptumEhr"

# Premier
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaPremier"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_premier_v2184"
workDatabaseSchema <- "scratch_mschuemi"
sampleTable <- "GeneralPretrainedModelTools_premier"
folder <- "d:/GeneralPretrainedModelTools_premier"

# CPRD
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaCprd"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_cprd_v2151"
workDatabaseSchema <- "scratch_mschuemi"
sampleTable <- "GeneralPretrainedModelTools_cprd"
folder <- "d:/GeneralPretrainedModelTools_cprd"

# Data fetch -------------------------------------------------------------------
extractCdmToParquet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  workDatabaseSchema = workDatabaseSchema,
  partitionTablePrefix = partitionTablePrefix,
  folder = folder,
  sampleSize = 100000,
  partitions = 10,
  maxCores = 4,
  forceRestart = F
) 
