# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of ApolloR
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

#' Create model training settings
#'
#' @param trainFraction   Fraction of the data to use for training. Set to 1 to skip evaluation.
#' @param numEpochs       Number of epochs to train for.
#' @param numFreezeEpochs Number of epochs to freeze the pre-trained model for.
#' @param learningRate    Learning rate for the Adam optimizer.
#' @param weightDecay     Weight decay for the Adam optimizer.
#'
#' @return
#' Training settings.
#' 
#' @export
createTrainingSettings <- function(trainFraction = 1.0, 
                                   numEpochs = 100,
                                   numFreezeEpochs = 1,
                                   learningRate = 0.001,
                                   weightDecay = 0.0) {
  settings <- list()
  for (name in names(formals(createTrainingSettings))) {
    settings[[SqlRender::camelCaseToSnakeCase(name)]] <- intToInteger(get(name))
  }
  return(settings)
}

#' Fine-tune a pretrained model
#'
#' @param pretrainedModelFolder The folder containing the pretrained model.
#' @param fineTunedModelFolder   The folder where the fine-tuned model will be written.
#' @param covariateData         The `CovariateData` object containing the CDM  data covariates.
#' @param labels                The labels to use for training. A data frame containing at least two
#'                              columns: `rowId` and `outcomeCount`. 
#' @param trainingSettings      Parameters used when training the model.
#' @param maxCores              The maximum number of CPU cores to use during fine-tuning. If a GPU 
#'                              is found (CUDA or MPS), it will automatically be used irrespective 
#'                              of the  setting of `maxCores`.
#'
#' @return
#' Does not return anything. Is called for the side effect of creating the fine-tuned model and 
#' saving it in the `fineTuneModel` folder.
#' 
#' @export
fineTuneModel <- function(pretrainedModelFolder,
                          fineTunedModelFolder,
                          covariateData,
                          labels,
                          trainingSettings = createTrainingSettings(),
                          maxCores = 5) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(pretrainedModelFolder, len = 1, add = errorMessages)
  checkmate::assertCharacter(fineTunedModelFolder, len = 1, add = errorMessages)
  # TODO: add check for covariate data
  checkmate::assertDataFrame(labels, add = errorMessages)
  checkmate::assertNames(names(labels), must.include = c("rowId", "outcomeCount"))
  checkmate::assertInt(maxCores, lower = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  parquetRootFolder <- attr(covariateData, "metaData")$parquetRootFolder
  writeLabelsToParquet(labels = labels,
                       parquetRootFolder = parquetRootFolder)
  mappingSettings <- yaml::read_yaml(file.path(pretrainedModelFolder, "cdm_mapping.yaml"))
  processCdmData(cdmDataPath = parquetRootFolder, 
                 personSequenceFolder = file.path(parquetRootFolder, "person_sequence"),
                 mappingSettings = mappingSettings,
                 hasLabels = TRUE,
                 maxCores = maxCores)
  trainModel(pretrainedModelFolder = pretrainedModelFolder,
             personSequenceFolder = file.path(parquetRootFolder, "person_sequence"),
             fineTunedModelFolder = fineTunedModelFolder,
             trainingSettings =trainingSettings)
  
}

#' Use a fine-tuned model to predict
#'
#' @param fineTunedModelFolder   The folder where the fine-tuned model was written by `fineTuneModel()`.
#' @param covariateData         The `CovariateData` object containing the CDM data covariates.
#' @param population            The population predict for. A data frame with at least one columns:
#'                              `rowId` and `outcomeCount`. 
#' @param maxCores              The maximum number of CPU cores to use during ine-tuning. If a GPU 
#'                              is found (CUDA or MPS), it ill automatically be used irrespective of 
#'                              the setting of `maxCores`.
#'
#' @return
#' Returns a data frame with the set of row IDs in `population`, and a columnm `prediction` 
#' containing the probability of belonging to the class.
#' 
#' @export
predictFineTuned <- function(fineTunedModelFolder,
                             covariateData,
                             population,
                             maxCores = 5) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(fineTunedModelFolder, len = 1, add = errorMessages)
  # TODO: add check for covariate data
  checkmate::assertDataFrame(population, add = errorMessages)
  checkmate::assertNames(names(population), must.include = c("rowId"))
  checkmate::assertInt(maxCores, lower = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  parquetRootFolder <- attr(covariateData, "metaData")$parquetRootFolder
  population <- population %>%
    mutate(outcomeCount = 0)
  writeLabelsToParquet(labels = population,
                       parquetRootFolder = parquetRootFolder)
  mappingSettings <- yaml::read_yaml(file.path(fineTunedModelFolder, "cdm_mapping.yaml"))
  personSequenceFolder <- file.path(parquetRootFolder, "person_sequence")
  processCdmData(cdmDataPath = parquetRootFolder, 
                 personSequenceFolder = personSequenceFolder,
                 mappingSettings = mappingSettings,
                 hasLabels = TRUE,
                 maxCores = maxCores)
  prediction <- predictModel(personSequenceFolder = personSequenceFolder,
                             fineTunedModelFolder = fineTunedModelFolder)
  return(prediction)
  
}

writeLabelsToParquet <- function(labels, parquetRootFolder) {
  labels <- labels %>%
    transmute(observation_period_id = .data$rowId, label = (.data$outcomeCount != 0))
  labelFolder <- file.path(parquetRootFolder, "label")
  if (dir.exists(labelFolder)) {
    # Already exist, probably from an earlier training or prediction
    unlink(labelFolder, recursive = TRUE)
  }
  dir.create(labelFolder)
  partitionFiles <- list.files(file.path(parquetRootFolder, "observation_period"), pattern = ".parquet$")
  for (partitionFile in partitionFiles) {
    op <- arrow::read_parquet(file.path(parquetRootFolder, "observation_period", partitionFile))
    labelPartition <- labels %>%
      inner_join(op %>%
                   select("observation_period_id"),
                 by = "observation_period_id")
    arrow::write_parquet(labelPartition, sink = file.path(labelFolder, partitionFile))  
  }
}

processCdmData <- function(cdmDataPath, 
                           personSequenceFolder, 
                           mappingSettings,
                           hasLabels = FALSE,
                           maxCores) {
  message("Processing CDM data")
  if (dir.exists(personSequenceFolder)) {
    # Already exist, probably from an earlier training or prediction
    unlink(personSequenceFolder, recursive = TRUE)
  }
  cdmProcessingSettings <- list(
    system = list(
      cdm_data_path = cdmDataPath,
      label_sub_folder = if (hasLabels) "label" else NULL,
      max_cores = as.integer(maxCores),
      output_path = personSequenceFolder
    ),
    mapping = mappingSettings,
    debug = list(
      profile = FALSE
    )
  )
  yamlFileName <- tempfile("cdm_processor", fileext = ".yaml")
  on.exit(unlink(yamlFileName))
  yaml::write_yaml(cdmProcessingSettings,
                   yamlFileName)
  
  reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  cdmProcessorModule <- reticulate::import("cdm_processing.cdm_processor")
  cdmProcessorModule$main(c(yamlFileName, ""))
}

trainModel <- function(pretrainedModelFolder,
                       personSequenceFolder,
                       fineTunedModelFolder,
                       trainingSettings) {
  dir.create(fineTunedModelFolder)
  modelSettings <- yaml::read_yaml(file.path(pretrainedModelFolder, "model.yaml"))
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = personSequenceFolder,
      output_folder= fineTunedModelFolder,
      pretrained_model_folder = pretrainedModelFolder,
      batch_size = as.integer(32),
      checkpoint_every = as.integer(10)
    ),
    learning_objectives = list(
      truncate_type = "tail",
      label_prediction = TRUE
    ),
    training = trainingSettings,
    model = modelSettings
  )
  yamlFileName <- tempfile("model_trainer", fileext = ".yaml")
  on.exit(unlink(yamlFileName))
  yaml::write_yaml(trainModelSettings,
                   yamlFileName)
  
  reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  trainModelModule <- reticulate::import("training.train_model")
  trainModelModule$main(c(yamlFileName, ""))
}

predictModel <- function(fineTunedModelFolder,
                         personSequenceFolder) {
  modelSettings <- yaml::read_yaml(file.path(fineTunedModelFolder, "model.yaml"))
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = personSequenceFolder,
      output_folder= fineTunedModelFolder,
      batch_size = as.integer(32),
      checkpoint_every = as.integer(10)
    ),
    learning_objectives = list(
      truncate_type = "tail",
      new_label_prediction = TRUE
    ),
    training = createTrainingSettings(trainFraction = 0),
    model = modelSettings
  )
  yamlFileName <- tempfile("predict", fileext = ".yaml")
  on.exit(unlink(yamlFileName))
  yaml::write_yaml(trainModelSettings,
                   yamlFileName)
  
  resultFileName <- tempfile("prediction", fileext = ".csv")
  on.exit(unlink(resultFileName), add = TRUE)
  reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  trainModelModule <- reticulate::import("training.train_model")
  trainModelModule$main(c(yamlFileName, resultFileName))
  # Workaround for issue https://github.com/tidyverse/vroom/issues/519:
  readr::local_edition(1)
  prediction <- readr::read_csv(resultFileName, show_col_types = FALSE)
  return(prediction)
}