
#' Apollo Finetuner
#' @param numEpochs Number of epochs to train the model.
#' @param numFreezeEpochs Number of epochs to freeze the pretrained model.
#' @param learningRate Learning rate for the optimizer.
#' @param weightDecay Weight decay for the optimizer.
#' @param batchSize Batch size for training.
#' @param predictionHead The type of prediction head to use. Options are "lstm" and "linear".
#' @param pretrainedModelFolder The folder containing the pretrained model.
#' @param parquetRootFolder The folder where the CDM data was written by the `extractCdmToParquet()` function.
#' @param personSequenceFolder The folder where the person sequence data was written by the 
#' `processCdmData()` function. If not specified `processCdmData()` will be called
#' @param maxCores The maximum number of CPU cores to use during fine-tuning. 
#' @param device The device to use for fine-tuning. Options are "cuda:x" where x is a number or "cpu".
#'@export
createApolloFinetuner <- function(numEpochs = 1,
                                  numFreezeEpochs = 1,
                                  learningRate = 3e-4,
                                  weightDecay = 1e-5,
                                  batchSize = 32,
                                  predictionHead = "lstm",
                                  pretrainedModelFolder = "path/to/model",
                                  parquetRootFolder = "path/to/data/",
                                  personSequenceFolder = NULL,
                                  maxCores = 1,
                                  device = "cuda") {
  # check inputs with checkmate
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assert_numeric(numEpochs, add = errorMessages)
  checkmate::assert_numeric(numFreezeEpochs, add = errorMessages)
  checkmate::assert_numeric(learningRate, add = errorMessages)
  checkmate::assert_numeric(weightDecay, add = errorMessages)
  checkmate::assert_numeric(batchSize, add = errorMessages)
  checkmate::assert_character(predictionHead, len = 1, add = errorMessages)
  checkmate::assert_character(pretrainedModelFolder, len = 1, add = errorMessages)
  pretrainedModelFolder <- normalizePath(pretrainedModelFolder)
  checkmate::assert_directory_exists(pretrainedModelFolder, add = errorMessages)
  parquetRootFolder <- normalizePath(parquetRootFolder)
  checkmate::assert_character(parquetRootFolder, len = 1, add = errorMessages)
  checkmate::assert_directory_exists(parquetRootFolder, add = errorMessages)
  if (!is.null(personSequenceFolder)) {
    personSequenceFolder <- normalizePath(personSequenceFolder)
    checkmate::assert_directory_exists(personSequenceFolder, add = errorMessages)
  }
  checkmate::assert_numeric(maxCores, add = errorMessages)
  checkmate::assert_character(device, len = 1, add = errorMessages)
  
  checkmate::reportAssertions(errorMessages)
  
  # parameters to use in gridSearch if more than one
  paramGrid <- list(
    learningRate = learningRate,
    weightDecay = weightDecay,
    predictionHead = predictionHead,
    numFreezeEpochs = numFreezeEpochs
  )
  param <- PatientLevelPrediction::listCartesian(paramGrid)
  
  results <- list(
    fitFunction = "ApolloR::finetune",
    dataFolder = parquetRootFolder,
    modelFolder = pretrainedModelFolder,
    sequenceFolder = personSequenceFolder,
    maxCores = maxCores,
    device = device,
    batchsize = batchSize,
    param = param,
    saveType = "file",
    modelParamNames = c("learningRate", "weightDecay", "predictionHead", "numFreezeEpochs"),
    modelType = "Apollo"
  )
  attr(results$param, "settings")$modelType <- results$modelType
  
  class(results) <- "modelSettings"
  return(results)
}

#' Apollo Finetuner
#' @param trainData The covariate data to use for fine-tuning the model.
#' @param modelSettings The model settings to use for fine-tuning the model.
#' @param analysisId The analysis ID to use for fine-tuning the model.
#' @param analysisPath The path to save the analysis to.
#' @export
finetune <- function(
    trainData,
    modelSettings,
    analysisId,
    analysisPath,
    ...
) {
  start <- Sys.time()
  
  if (!is.null(trainData$folds)) {
    trainData$labels <- merge(trainData$labels, trainData$fold, by = "rowId")
  }
  
  browser()
  if (is.null(modelSettings$sequenceFolder)) {
    mappingSettings <- yaml::read_yaml(file.path(modelSettings$modelFolder, "cdm_mapping.yaml"))
    modelSettings$sequenceFolder <- tempfile()
    processCdmData(cdmDataPath = modelSettings$dataFolder, 
                   personSequenceFolder = modelSettings$sequenceFolder,
                   mappingSettings = mappingSettings,
                   maxCores = modelSettings$maxCores)
  }
  # largest non-test fold is validation, rest of positive folds is training
  isTrainingCondition <- (trainData$labels$index != max(trainData$labels$index)) & (trainData$labels$index > 0)
  labels <- trainData$labels %>% 
    dplyr::mutate(isTraining = isTrainingCondition) %>%
    dplyr::select(.data$subjectId, .data$outcomeCount, .data$isTraining) %>%
    dplyr::rename(rowId = "subjectId")
    
  writeLabelsToParquet(labels = labels, 
                       parquetRootFolder = modelSettings$dataFolder,
                       labelFolder = file.path(analysisPath, "labels"))
  
  preTrainedModelSettings <- yaml::read_yaml(file.path(modelSettings$modelFolder, "model.yaml"))
  trainingSettings <- list(
    train_fraction = "plp",
    num_epochs = modelSettings$param$numEpochs,
    num_freeze_epochs = modelSettings$param$numFreezeEpochs,
    learning_rate = modelSettings$param$learningRate,
    weight_decay = modelSettings$param$weightDecay,
    max_batches = NULL
  )
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = modelSettings$sequenceFolder,
      output_folder = analysisPath,
      pretrained_model_folder = pretrainedModelFolder,
      batch_size = modelSettings$param$bathSize,
      checkpoint_every = as.integer(1)
    ),
    learning_objectives = list(
      truncate_type = "tail",
      predict_new = FALSE,
      label_prediction = tolower(modelSettings$param$predictionHead) != "lstm",
      lstm_label_prediction = tolower(modelSettings$param$predictionHead) == "lstm"
    ),
    training = trainingSettings,
    model = preTrainedModelSettings
  )
  yamlFileName <- tempfile("model_trainer", fileext = ".yaml")
  on.exit(unlink(yamlFileName))
  yaml::write_yaml(trainModelSettings,
                   yamlFileName)
  
  # reticulate::use_virtualenv("apollo")
  ensurePythonFolderSet()
  
  trainModelModule <- reticulate::import("training.train_model")
  trainModelModule$main(c(yamlFileName, ""))
  yaml::write_yaml(trainModelSettings$learning_objectives,
                   file.path(fineTunedModelFolder, "learning_objectives.yaml")) 
}