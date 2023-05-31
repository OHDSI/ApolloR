# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of GeneralPretrainedModelTools
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

# library(dplyr)
# source("R/HelperFunctions.R")

#' Compute descriptive statistics for Parquet files
#'
#' @description
#' Computes descriptive statistics for data extracted using the [extractCdmToParquet()]
#' function. It will produce two CSV tables in the `folder`:
#'
#' - **TableDescriptives.csv** will contain the row count and, where applicable, the
#' person count per table.
#' - **ConceptDescriptives.csv** will contain, for each concept, the number of occurrences,
#' and the number of persons having that concept.
#'
#' @param folder  The folder on the local file system where the Parquet files were
#'                written.
#'
#' @return
#' Does not return anything. Is called for the side-effect of generating the two
#' CSV files.
#'
#' @export
computeParquetDescriptives <- function(folder) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(folder, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  tableColumns <- getTableColumnsToExtract()
  
  # Concept descriptives -------------------------------------------------------
  message("Computing concept descriptives")
  tableColumnsSubset <- tableColumns %>%
    filter(.data$conceptDescriptives == "yes") %>%
    group_by(.data$cdmTableName, .data$cdmFieldName) %>%
    group_split(.keep = TRUE)
  
  conceptDescriptives <- lapply(tableColumnsSubset, computeConceptDescriptives, folder = folder)
  conceptDescriptives <- bind_rows(conceptDescriptives)
  concept <- arrow::open_dataset(file.path(folder, "concept"))
  conceptNames <- concept %>%
    select(conceptId = "concept_id", conceptName = "concept_name") %>%
    filter(.data$conceptId %in% conceptDescriptives$conceptId) %>%
    collect()
  conceptDescriptives <- conceptDescriptives %>%
    inner_join(conceptNames, by = join_by("conceptId")) %>%
    select("cdmTableName", "cdmFieldName", "conceptId", "conceptName", "conceptCount", "personCount") %>%
    arrange(desc(.data$personCount))
  readr::write_csv(conceptDescriptives, file.path(folder, "ConceptDescriptives.csv"))
  message("Concept descriptives saved to ", file.path(folder, "ConceptDescriptives.csv"))
  
  # Table descriptives ---------------------------------------------------------
  message("Computing table descriptives")
  tables <- tableColumns %>%
    distinct(cdmTableName) %>%
    pull()
  tableDescriptives <- lapply(tables, computeTableDescriptives, folder = folder)
  tableDescriptives <- bind_rows(tableDescriptives)
  readr::write_csv(tableDescriptives, file.path(folder, "TableDescriptives.csv"))
  message("Table descriptives saved to ", file.path(folder, "TableDescriptives.csv"))
}

# row = tablesConceptDescriptives[[6]]
computeConceptDescriptives <- function(row, folder) {
  if (!dir.exists(file.path(folder, row$cdmTableName))) {
    warning(sprintf("Cannot find table %s, skipping", row$cdmTableName))
    return(NULL)
  }
  message(sprintf("- Computing concept descriptives for field %s in table %s", row$cdmFieldName, row$cdmTableName))
  data <- arrow::open_dataset(file.path(folder, row$cdmTableName))
  columnIdx <- which(names(data) == row$cdmFieldName)
  # Weird dplyr syntax to select a column by name (stored in a variable):
  descriptives <- data %>%
    select(x = all_of(columnIdx), "person_id") %>%
    group_by(x) %>%
    summarise(conceptCount = n(), personCount = n_distinct(.data$person_id)) %>%
    ungroup() %>%
    collect()
  descriptives <- descriptives %>%
    mutate(
      cdmTableName = as.factor(row$cdmTableName),
      cdmFieldName = as.factor(row$cdmFieldName)
    ) %>%
    rename(conceptId = "x")
  return(descriptives)
}

computeTableDescriptives <- function(table, folder) {
  if (!dir.exists(file.path(folder, table))) {
    warning(sprintf("Cannot find table %s, skipping", table))
    return(NULL)
  }
  message(sprintf("- Computing table descriptives for table %s", table))
  data <- arrow::open_dataset(file.path(folder, table))
  descriptives <- tibble(
    cdmTableName = table,
    rowCount = nrow(data)
  )
  if ("person_id" %in% names(data)) {
    personDescriptives <- data %>%
      group_by(.data$person_id) %>%
      summarise(recordCount = n()) %>%
      ungroup() %>%
      summarise(
        personCount = n(),
        meanRecordsPerPerson = mean(.data$recordCount),
        minRecordsPerPerson = min(.data$recordCount),
        p5RecordsPerPerson = quantile(.data$recordCount, 0.05),
        p25RecordsPerPerson = quantile(.data$recordCount, 0.25),
        medianRecordsPerPerson = median(.data$recordCount),
        p75RecordsPerPerson = quantile(.data$recordCount, 0.75),
        p95RecordsPerPerson = quantile(.data$recordCount, 0.95),
        maxRecordsPerPerson = max(.data$recordCount)
      ) %>%
      collect()
    descriptives <- bind_cols(descriptives, personDescriptives)
  }
  return(descriptives)
}
