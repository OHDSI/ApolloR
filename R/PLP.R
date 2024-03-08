
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
#' `processCdmData()` function. 
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
    numEpochs = numEpochs,
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
  # largest non-test fold is validation, rest of positive folds is training
  isTrainingCondition <- (trainData$labels$index != max(trainData$labels$index)) & (trainData$labels$index > 0)
  labels <- trainData$labels %>% 
    dplyr::mutate(is_training = isTrainingCondition) %>%
    dplyr::select(.data$subjectId, .data$outcomeCount, .data$is_training) %>%
    dplyr::rename(rowId = "subjectId")
   
  if (is.null(modelSettings$sequenceFolder)) {
    modelSettings$sequenceFolder <- tempfile()
    writeLabelsToParquet(labels = labels, 
                         parquetRootFolder = modelSettings$dataFolder,
                         labelFolder = file.path(analysisPath, "labels"))
    mappingSettings <- yaml::read_yaml(file.path(modelSettings$modelFolder, "cdm_mapping.yaml"))
    modelSettings$sequenceFolder <- tempfile()
    processCdmData(cdmDataPath = modelSettings$dataFolder, 
                   personSequenceFolder = modelSettings$sequenceFolder,
                   mappingSettings = mappingSettings,
                   labels = file.path(analysisPath, "labels"),
                   maxCores = modelSettings$maxCores)
    outDir <- file.path(analysisPath, "sequences")
    if (!dir.exists(outDir)) {
      dir.create(outDir, recursive = TRUE)      
    }
    status <- file.copy(from = file.path(modelSettings$sequenceFolder, dir(modelSettings$sequenceFolder)), 
                        to = outDir, 
                        recursive = TRUE)
    if (!all(status)) {
      stop("Failed to copy sequence folder to analysis path")
    }
  } else if (dir.exists(file.path(analysisPath, "sequences"))) {
      # skip processing if sequences already exist 
  } else {
    # I've given the dir with all the person sequences of the whole db
    # filter down to the prediction problem
    ensurePythonFolderSet()
    utils <- reticulate::import("cdm_processing.cdm_processor_utils")
    
    utils$filter_prediction_problem(modelSettings$sequenceFolder,
                                    reticulate::r_to_py(labels),
                                    analysisPath)
    file.copy(from = file.path(modelSettings$sequenceFolder, "cdm_mapping.yaml"),
              to = file.path(analysisPath, "sequences","cdm_mapping.yaml"))
  }
  browser()
  preTrainedModelSettings <- yaml::read_yaml(file.path(modelSettings$modelFolder, "model.yaml"))
  param <- modelSettings$param[[1]]
  trainingSettings <- list(
    train_fraction = "plp",
    num_epochs = as.integer(modelSettings$numEpochs),
    num_freeze_epochs = as.integer(param$numFreezeEpochs),
    learning_rate = param$learningRate,
    weight_decay = param$weightDecay,
    max_batches = 10L
  )
  trainModelSettings <- list(
    system = list(
      sequence_data_folder = normalizePath(file.path(analysisPath, "sequences")),
      output_folder = analysisPath,
      pretrained_model_folder = modelSettings$modelFolder,
      batch_size = as.integer(modelSettings$batchsize),
      checkpoint_every = as.integer(1),
      writer = "json"
    ),
    learning_objectives = list(
      truncate_type = "tail",
      predict_new = FALSE,
      label_prediction = tolower(param$predictionHead) != "lstm",
      lstm_label_prediction = tolower(param$predictionHead) == "lstm"
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
