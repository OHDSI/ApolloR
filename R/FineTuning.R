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

fineTuneModel <- function(pretrainedModelFolder,
                          fineTuneModelFolder,
                          covariateData,
                          labels,
                          maxCores = 5) {
  parquetRootFolder <- attr(covariateData, "metaData")$parquetRootFolder
  writeLabelsToParquet(labels = labels,
                       parquetRootFolder = parquetRootFolder)
  processCdmData(cdmDataPath = parquetRootFolder, 
                 personSequenceFolder = file.path(cdmDataPath, "person_sequence"),
                 pretrainedModelFolder = pretrainedModelFolder,
                 maxCores = maxCores)
  trainModel(pretrainedModelFolder = pretrainedModelFolder,
             personSequenceFolder = file.path(cdmDataPath, "person_sequence"),
             fineTuneModelFolder = fineTuneModelFolder)
  
}

writeLabelsToParquet <- function(labels, parquetRootFolder) {
  labels <- labels %>%
    transmute(observation_period_id = .data$rowId, label = (outcomeCount != 0))
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

processCdmData <- function(cdmDataPath, personSequenceFolder, pretrainedModelFolder, maxCores) {
  if (dir.exists(personSequenceFolder)) {
    # Already exist, probably from an earlier training or prediction
    unlink(personSequenceFolder, recursive = TRUE)
  }
  # Use same mapping settings as used to create pretrained model:
  mapping <- yaml::read_yaml(file.path(pretrainedModelFolder, "cdm_mapping.yaml"))
  cdmProcessingSettings <- list(
    system = list(
      cdm_data_path = cdmDataPath,
      label_sub_folder = "label",
      max_cores = as.integer(maxCores),
      output_path = personSequenceFolder
    ),
    mapping = mapping,
    debug = list(
      profile = FALSE
    )
  )
  yamlFileName <- file.path(parquetRootFolder, "cdm_processor.yaml")
  yaml::write_yaml(cdmProcessingSettings,
                   yamlFileName)
  
  reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  cdmProcessorModule <- reticulate::import("cdm_processing.cdm_processor")
  cdmProcessorModule$main(yamlFileName)
}

trainModel <- function(pretrainedModelFolder,
                       personSequenceFolder,
                       fineTuneModelFolder) {
  dir.create(fineTuneModelFolder)
  modelSettings <- yaml::read_yaml(file.path(pretrainedModelFolder, "model.yaml"))
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = personSequenceFolder,
      output_folder= fineTuneModelFolder,
      pretrained_model_folder = pretrainedModelFolder,
      batch_size = 32,
      checkpoint_every = 10
    ),
    learning_objectives = list(
      truncate_type = "tail",
      label_prediction = TRUE
    ),
    training = list(
      train_fraction = 0.8, 
      num_epochs = 100,
      num_freeze_epochs = 1,
      learning_rate = 0.001,
      weight_decay = 0.01
    ),
    model = modelSettings
  )
  yamlFileName <- file.path(fineTuneModelFolder, "model_trainer.yaml")
  yaml::write_yaml(cdmProcessintrainModelSettingsgSettings,
                   yamlFileName)
  
  reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  trainModelModule <- reticulate::import("training.train_model")
  trainModelModule$main(yamlFileName)
}