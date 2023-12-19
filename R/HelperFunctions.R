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

getTableColumnsToExtract <- function() {
  pathToCsv <- system.file("tableColumnsToExtract.csv", package = "ApolloR")
  tableColumnsToExtract <- read.csv(pathToCsv)
  return(tableColumnsToExtract)
}

#' Get path to Python's requirements.txt 
#'
#' @return
#' The path
#' 
#' @export
getPythonRequirementsFilePath <- function() {
  return(system.file("python", "requirements.txt", package = "ApolloR"))
}

ensurePythonFolderSet <- function() {
  pythonFolder <- system.file("python", package = "ApolloR")
  sys <- reticulate::import("sys")
  if (!pythonFolder %in% sys$path) {
    reticulate::py_run_string(paste0("import sys; sys.path.append('", pythonFolder, "')"))
  }
}