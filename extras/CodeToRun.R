library(ApolloR)

maxCores <- min(8, parallel::detectCores())

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
folder <- "d:/GPM_MDCD_2"
sampleSize <- 1e5
partitions <- 100

# CCAE
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_truven_ccae_v2633"
workDatabaseSchema <- "scratch_mschuemi"
partitionTablePrefix <- "GPM_CCAE"
folder <- "d:/GPM_CCAE"
sampleSize <- 2e6
partitions <- 200


# CCAE (linux)
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = !!keyring::key_get("redShiftConnectionStringOhdaCcae"),
  user = !!keyring::key_get("redShiftUserName"),
  password = !!keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_truven_ccae_v2182"
workDatabaseSchema <- "scratch_mschuemi"
partitionTablePrefix <- "GPM_CCAE"
folder <- "/data/gpm_CCAE"
sampleSize <- 2e6
partitions <- 200


# Data fetch -------------------------------------------------------------------
extractCdmToParquet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  workDatabaseSchema = workDatabaseSchema,
  partitionTablePrefix = partitionTablePrefix,
  folder = folder,
  sampleSize = sampleSize,
  partitions = partitions,
  maxCores = maxCores,
  forceRestart = TRUE
) 

# Descriptives -----------------------------------------------------------------
computeParquetDescriptives(folder = folder)
