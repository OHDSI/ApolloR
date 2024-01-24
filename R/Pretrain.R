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

#' Create model settings
#'
#' @param maxSequenceLength           The maximum length of the input sequence.
#' @param conceptEmbedding            Use concept embedding?
#' @param visitOrderEmbedding         Use visit order embedding?
#' @param segmentEmbedding            Use segment embedding?
#' @param ageEmbedding                Use age embedding?
#' @param dateEmbedding               Use date embedding?
#' @param visitConceptEmbedding       Use visit concept embedding?
#' @param hiddenSize                  Size of hidden layers.
#' @param numAttentionHeads           Number of attention heads.
#' @param numHiddenLayers             Number of hidden layers.
#' @param intermediateSize            Size of intermediate layers.
#' @param hiddenAct                   Type of activation function used in the intermediate layer. Can be "gelu" or "relu".
#' @param embeddingCombinationMethod  Type of embedding combination method. Can be "sum" or "concat".
#' @param hiddenDropoutProb           Dropout probability for the hidden layers.
#' @param attentionProbsDropoutProb   Dropout probability for the attention layer.
#'
#' @return
#' A model settings object.
#' 
#' @export
createModelSettings <- function(maxSequenceLength = 512,
                                conceptEmbedding = TRUE,
                                visitOrderEmbedding = TRUE,
                                segmentEmbedding = TRUE,
                                ageEmbedding = TRUE,
                                dateEmbedding = TRUE,
                                visitConceptEmbedding = TRUE,
                                hiddenSize = 768,
                                numAttentionHeads = 12,
                                numHiddenLayers = 12,
                                intermediateSize = 3072,
                                hiddenAct = "gelu",
                                embeddingCombinationMethod = "sum",
                                hiddenDropoutProb = 0.1,
                                attentionProbsDropoutProb = 0.1) {
  settings <- list()
  for (name in names(formals(createModelSettings))) {
    settings[[SqlRender::camelCaseToSnakeCase(name)]] <- intToInteger(get(name))
  }
  return(settings)
}

createMappingSettings <- function(mapDrugsToIngredients = FALSE,
                                  conceptsToRemove = c(0, 900000010)) {
  settings <- list()
  for (name in names(formals(createMappingSettings))) {
    settings[[SqlRender::camelCaseToSnakeCase(name)]] <- intToInteger(get(name))
  }
  return(settings)
}

intToInteger <- function(value) {
  if (all(is.numeric(value)) && all(round(value) == value)) {
    return(as.integer(value))
  } else {
    return(value)
  }
}


#' Pretrain a model
#' 
#' @details
#' Note: primarily intended for debugging purposes. I would expect you'd pretrain the model in Python.
#' 
#'
#' @param parquetFolder         The folder where the CDM data was written by the
#'                              `extractCdmToParquet()` function.
#' @param pretrainedModelFolder The folder containing the pretrained model.
#' @param mappingSettings       Settings mapping CDM data to sequence data.
#' @param modelSettings         Model settings as created using `createModelSettings()`.
#' @param maxCores              The maximum number of CPU cores to use during pretraining. If a GPU 
#'                              is found (CUDA or MPS), it will automatically be used irrespective 
#'                              of the setting of `maxCores`.
#'
#' @return
#' Does not return anything. Is called for the side effect of creating the pretrained model and 
#' saving it in the `pretrainedModelFolder` folder.
#' 
#' @export
pretrainModel <- function(parquetFolder,
                          pretrainedModelFolder,
                          mappingSettings = createMappingSettings(),
                          modelSettings = createModelSettings(),
                          maxCores = 5) {
  personSequenceFolder <- file.path(parquetFolder, "person_sequence")
  processCdmData(cdmDataPath = parquetFolder, 
                 personSequenceFolder = personSequenceFolder,
                 mappingSettings = mappingSettings,
                 maxCores = maxCores)
  message("Pretraining model")
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = personSequenceFolder,
      output_folder= pretrainedModelFolder,
      batch_size = as.integer(4),
      checkpoint_every = as.integer(1)
    ),
    learning_objectives = list(
      truncate_type = "random",
      masked_concept_learning = TRUE
    ),
    training = list(
      train_fraction = 0.8, 
      num_epochs = as.integer(1),
      num_freeze_epochs = as.integer(0),
      learning_rate = 0.001,
      weight_decay = 0.0
    ),
    model = modelSettings
  )
  yamlFileName <- tempfile("model_trainer", fileext = ".yaml")
  on.exit(unlink(yamlFileName))
  yaml::write_yaml(trainModelSettings,
                   yamlFileName)
  
  # reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  trainModelModule <- reticulate::import("training.train_model")
  trainModelModule$main(c(yamlFileName, ""))
}