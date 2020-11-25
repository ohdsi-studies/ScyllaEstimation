# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of ScyllaEstimation
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

#' Run CohortMethod package
#'
#' @details
#' Run the CohortMethod package, which implements the comparative cohort design.
#'
#' @param connectionDetails      An object of type \code{connectionDetails} as created using the
#'                               \code{\link[DatabaseConnector]{createConnectionDetails}} function in
#'                               the DatabaseConnector package.
#' @param cdmDatabaseSchema      Schema name where your patient-level data in OMOP CDM format resides.
#'                               Note that for SQL Server, this should include both the database and
#'                               schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema   Schema name where intermediate data can be stored. You will need to
#'                               have write priviliges in this schema. Note that for SQL Server, this
#'                               should include both the database and schema name, for example
#'                               'cdm_data.dbo'.
#' @param cohortTable            The name of the table that will be created in the work database
#'                               schema. This table will hold the exposure and outcome cohorts used in
#'                               this study.
#' @param oracleTempSchema       Should be used in Oracle to specify a schema where the user has write
#'                               priviliges for storing temporary tables.
#' @param outputFolder           Name of local folder where the results were generated; make sure to
#'                               use forward slashes (/). Do not use a folder on a network drive since
#'                               this greatly impacts performance.
#' @param maxCores               How many parallel cores should be used? If more cores are made
#'                               available this can speed up the analyses.
#'
#' @export
runCohortMethod <- function(connectionDetails,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            oracleTempSchema,
                            minExposureCount = 100,
                            outputFolder,
                            maxCores) {
  cmOutputFolder <- file.path(outputFolder, "cmOutput")
  if (!file.exists(cmOutputFolder)) {
    dir.create(cmOutputFolder)
  }
  cmAnalysisListFile <- system.file("settings",
                                    "cmAnalysisList.json",
                                    package = "ScyllaEstimation")
  cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)
  tcosList <- createTcos(outputFolder = outputFolder)
  outcomesOfInterest <- getOutcomesOfInterest()

  # get exposure cohort IDs with counts >= 5
  pathToCsv <- file.path(outputFolder, "cohort_count.csv")
  cohortsForAnalysis <- readr::read_csv(pathToCsv, col_types = readr::cols())
  colnames(cohortsForAnalysis) <- SqlRender::snakeCaseToCamelCase(colnames(cohortsForAnalysis))
  cohortsForAnalysis <- cohortsForAnalysis[!is.na(cohortsForAnalysis$cohortSubjects), ]
  targetSubgroupCohortIds <- ScyllaCharacterization::getTargetSubgroupXref()$cohortId
  exposureKeeps <- cohortsForAnalysis$cohortId %in% targetSubgroupCohortIds & cohortsForAnalysis$cohortSubjects >= minExposureCount
  exposureIdsForAnalysis <- cohortsForAnalysis$cohortId[exposureKeeps]

  for (analysisDesign in c(100, 200, 300, 400)) {

    # 1. subgroup (with 365d prior obs) where cohort start date = target cohort start date (protocol figure 1) => 100
    # 2. subgroup (no prior obs required) where cohort start date = target cohort start date (protocol figure 2) => 200
    # 3. subgroup (with 365d prior obs) where cohort start date is between -7d to 0d relative to target cohort start date (protocol figure 3) => 300
    # 4. cohorts for protocol figure 4 are already implemented with subgroupId=2002 => 400

    # getDesign <- function(x) {
    #   tcoDesign <- as.integer(
    #     sub(pattern = "[[:digit:]]{4}", replacement = "",
    #         sub(pattern = "1$", replacement = "", x$targetId)))
    #   if (tcoDesign == 1) {
    #     return(100)
    #   } else if (tcoDesign == 2) {
    #     return(200)
    #   } else if (tcoDesign == 3) {
    #     return(300)
    #   } else if (tcoDesign == 2002) {
    #     return(400)
    #   } else {
    #     stop(paste("Unknown analysis plan for targetId", x$targetId))
    #   }
    # }

    subTcosList <- Filter(function(x) { getDesign(x$targetId) == analysisDesign },
                          tcosList)

    checkMinExposureCounts <- function(subTcos) {
      if (subTcos$targetId %in% exposureIdsForAnalysis & subTcos$comparatorId %in% exposureIdsForAnalysis) {
        return(subTcos)
      } else {
        return(NULL)
      }
    }

    subTcosList <- lapply(subTcosList, checkMinExposureCounts)
    subTcosList <- subTcosList[-which(sapply(subTcosList, is.null))]

    subCmAnalysisList <- Filter(function(x) { x$analysisId > analysisDesign && x$analysisId < (analysisDesign + 100) },
                                cmAnalysisList)

    if (length(subTcosList) > 0) {
      results <- CohortMethod::runCmAnalyses(connectionDetails = connectionDetails,
                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                             exposureDatabaseSchema = cohortDatabaseSchema,
                                             exposureTable = cohortTable,
                                             outcomeDatabaseSchema = cohortDatabaseSchema,
                                             outcomeTable = cohortTable,
                                             outputFolder = cmOutputFolder,
                                             oracleTempSchema = oracleTempSchema,
                                             cmAnalysisList = subCmAnalysisList,
                                             targetComparatorOutcomesList = subTcosList,
                                             getDbCohortMethodDataThreads = min(3, maxCores),
                                             createStudyPopThreads = min(3, maxCores),
                                             createPsThreads = max(1, round(maxCores/10)),
                                             psCvThreads = min(10, maxCores),
                                             trimMatchStratifyThreads = min(10, maxCores),
                                             fitOutcomeModelThreads = max(1, round(maxCores/4)),
                                             outcomeCvThreads = min(4, maxCores),
                                             refitPsForEveryOutcome = FALSE,
                                             outcomeIdsOfInterest = outcomesOfInterest)
      saveRDS(results, file.path(cmOutputFolder, sprintf("outcomeModelReference_%s.rds", analysisDesign)))
      summaryFile <- file.path(outputFolder, sprintf("analysisSummary_%s.rds", analysisDesign))
      if (!file.exists(summaryFile)) {
        ParallelLogger::logInfo("Summarizing results")
        analysisSummary <- CohortMethod::summarizeAnalyses(referenceTable = results, outputFolder = cmOutputFolder)
        saveRDS(analysisSummary, summaryFile)
      }
    }
  }
  analysisSummary <- lapply(file.path(outputFolder, sprintf("analysisSummary_%s.rds", c(100, 200, 300, 400))),
                            function(x) if (file.exists(x)) readRDS(x))
  analysisSummary <- bind_rows(analysisSummary)
  cohorts <- ScyllaCharacterization::getAllStudyCohorts()
  negativeControls <- getNegativeControlOutcomes()
  cohorts <-bind_rows(cohorts,
                      tibble(name = negativeControls$outcomeName,
                             cohortId = negativeControls$outcomeId))
  analysisDescription <- lapply(cmAnalysisList, function(x) dplyr::tibble(analysisId = x$analysisId, description = x$description))
  analysisDescription <- bind_rows(analysisDescription)

  analysisSummary <- analysisSummary %>%
    inner_join(select(cohorts, targetId = .data$cohortId, targetName = .data$name), by = "targetId") %>%
    inner_join(select(cohorts, comparatorId = .data$cohortId, comparatorName = .data$name), by = "comparatorId") %>%
    inner_join(select(cohorts, outcomeId = .data$cohortId, outcomeName = .data$name), by = "outcomeId") %>%
    inner_join(analysisDescription, by = "analysisId")

  readr::write_csv(analysisSummary, file.path(outputFolder, "analysisSummary.csv"))

  outcomeModelReference <- lapply(file.path(cmOutputFolder, sprintf("outcomeModelReference_%s.rds", c(100, 200, 300, 400))),
                                  function(x) if (file.exists(x)) readRDS(x))
  outcomeModelReference <- bind_rows(outcomeModelReference)
  saveRDS(outcomeModelReference, file.path(cmOutputFolder, "outcomeModelReference.rds"))
}

# 1. subgroup (with 365d prior obs) where cohort start date = target cohort start date (protocol figure 1) => 100
# 2. subgroup (no prior obs required) where cohort start date = target cohort start date (protocol figure 2) => 200
# 3. subgroup (with 365d prior obs) where cohort start date is between -7d to 0d relative to target cohort start date (protocol figure 3) => 300
# 4. cohorts for protocol figure 4 are already implemented with subgroupId=2002 => 400

getDesign <- function(targetId) {
  tcoDesign <- as.integer(
    sub(pattern = "[[:digit:]]{4}", replacement = "",
        sub(pattern = "1$", replacement = "", targetId)))
  if (tcoDesign == 1) {
    return(100)
  } else if (tcoDesign == 2) {
    return(200)
  } else if (tcoDesign == 3) {
    return(300)
  } else if (tcoDesign == 2002) {
    return(400)
  } else {
    stop(paste("Unknown analysis plan for targetId", targetId))
  }
}


addAnalysisDescription <- function(data,
                                   IdColumnName = "analysisId",
                                   nameColumnName = "analysisDescription") {
  cmAnalysisListFile <- system.file("settings",
                                    "cmAnalysisList.json",
                                    package = "ScyllaEstimation")
  cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)
  idToName <- lapply(cmAnalysisList, function(x) data.frame(analysisId = x$analysisId,
                                                            description = as.character(x$description)))
  idToName <- do.call("rbind", idToName)
  names(idToName)[1] <- IdColumnName
  names(idToName)[2] <- nameColumnName
  data <- merge(data, idToName, all.x = TRUE)
  # Change order of columns:
  idCol <- which(colnames(data) == IdColumnName)
  if (idCol < ncol(data) - 1) {
    data <- data[, c(1:idCol, ncol(data), (idCol + 1):(ncol(data) - 1))]
  }
  return(data)
}

createTcos <- function(outputFolder) {
  pathToCsv <- system.file("settings",
                           "TcosOfInterest.csv",
                           package = "ScyllaEstimation")
  tcosOfInterest <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  negativeControls <- getNegativeControlOutcomes()
  tcs <- unique(tcosOfInterest[ ,c("targetId", "comparatorId")])
  createTco <- function(i) {
    targetId <- tcs$targetId[i]
    comparatorId <- tcs$comparatorId[i]
    outcomeIds <- as.character(tcosOfInterest$outcomeIds[tcosOfInterest$targetId == targetId & tcosOfInterest$comparatorId ==
                                                           comparatorId])
    outcomeIds <- as.numeric(strsplit(outcomeIds, split = ";")[[1]])
    outcomeIds <- c(outcomeIds,
                    negativeControls$outcomeId)
    excludeConceptIds <- as.character(tcosOfInterest$excludedCovariateConceptIds[tcosOfInterest$targetId ==
                                                                                   targetId & tcosOfInterest$comparatorId == comparatorId])
    if (length(excludeConceptIds) == 1 && is.na(excludeConceptIds)) {
      excludeConceptIds <- c()
    } else if (length(excludeConceptIds) > 0) {
      excludeConceptIds <- as.numeric(strsplit(excludeConceptIds, split = ";")[[1]])
    }
    includeConceptIds <- as.character(tcosOfInterest$includedCovariateConceptIds[tcosOfInterest$targetId ==
                                                                                   targetId & tcosOfInterest$comparatorId == comparatorId])
    if (length(includeConceptIds) == 1 && is.na(includeConceptIds)) {
      includeConceptIds <- c()
    } else if (length(includeConceptIds) > 0) {
      includeConceptIds <- as.numeric(strsplit(includeConceptIds, split = ";")[[1]])
    }
    tco <- CohortMethod::createTargetComparatorOutcomes(targetId = targetId,
                                                        comparatorId = comparatorId,
                                                        outcomeIds = outcomeIds,
                                                        excludedCovariateConceptIds = excludeConceptIds,
                                                        includedCovariateConceptIds = includeConceptIds)
    return(tco)
  }
  tcosList <- lapply(1:nrow(tcs), createTco)
  return(tcosList)
}

getOutcomesOfInterest <- function() {
  pathToCsv <- system.file("settings",
                           "TcosOfInterest.csv",
                           package = "ScyllaEstimation")
  tcosOfInterest <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  outcomeIds <- as.character(tcosOfInterest$outcomeIds)
  outcomeIds <- do.call("c", (strsplit(outcomeIds, split = ";")))
  outcomeIds <- unique(as.numeric(outcomeIds))
  return(outcomeIds)
}

getNegativeControlOutcomes <- function() {
  pathToCsv <- system.file("settings",
                           "NegativeControlConceptIds.csv",
                           package = "ScyllaEstimation")
  return(readr::read_csv(pathToCsv, col_types = readr::cols()))
}
