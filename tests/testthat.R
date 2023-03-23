library(testthat)
library(GeneralPretrainedModelTools)
if (Sys.getenv("CDM5_POSTGRESQL_SERVER") != "") {
  test_check("GeneralPretrainedModelTools")
} else {
  message("Skipping testing because environmental variables not set")
}
