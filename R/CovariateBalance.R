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

#' Compute covariate balance
#'
#' @details
#' Compute covariate balance
#'
#' @param outputFolder           Name of local folder where the results were generated; make sure to
#'                               use forward slashes (/). Do not use a folder on a network drive since
#'                               this greatly impacts performance.
#' @param maxCores               How many parallel cores should be used? If more cores are made
#'                               available this can speed up the analyses.
#'
#' @export
computeCovariateBalance <- function(outputFolder,
                                    maxCores) {
  cmOutputFolder <- file.path(outputFolder, "cmOutput")
  if (!file.exists(cmOutputFolder)) {
    dir.create(cmOutputFolder)
  }
  cmAnalysisListFile <- system.file("settings",
                                    "cmAnalysisList.json",
                                    package = "ScyllaEstimation")
  cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)

  balanceFolder <- file.path(outputFolder, "balance")
  if (!file.exists(balanceFolder)) {
    dir.create(balanceFolder)
  }
  outcomeModelReference <- readRDS(file.path(cmOutputFolder, "outcomeModelReference.rds"))
  subset <- outcomeModelReference %>%
    filter(.data$sharedPsFile != "") %>%
    distinct(.data$analysisId,
             .data$targetId,
             .data$comparatorId,
             .data$outcomeId,
             .data$cohortMethodDataFile,
             .data$sharedPsFile,
             .data$strataFile)
  if (nrow(subset) > 0) {
    subset <- split(subset, paste(subset$targetId, subset$comparatorId, subset$analysisId))
    cluster <- ParallelLogger::makeCluster(min(3, maxCores))
    invisible(ParallelLogger::clusterApply(cluster,
                                           subset,
                                           computeTCACovariateBalance,
                                           cmOutputFolder = cmOutputFolder,
                                           balanceFolder = balanceFolder,
                                           cmAnalysisList = cmAnalysisList))

    ParallelLogger::stopCluster(cluster)
  }
}

computeTCACovariateBalance <- function(rows, cmOutputFolder, balanceFolder, cmAnalysisList) {
  # rows = subset[[1]]

  cohortMethodData <- NULL
  # Per TCA: compute balance for all covariates ---------------------------------------------------
  outputFileName <- file.path(balanceFolder, sprintf("bal_t%d_c%d_a%d.rds",
                                                     rows$targetId[1],
                                                     rows$comparatorId[1],
                                                     rows$analysisId[1]))
  if (!file.exists(outputFileName)) {
    ParallelLogger::logTrace("Creating covariate balance file ", outputFileName)
    psFile <- file.path(cmOutputFolder, rows$sharedPsFile[1])
    ps <- readRDS(psFile)
    for (cmAnalysis in cmAnalysisList) {
      if (cmAnalysis$analysisId == rows$analysisId[1])
        break
    }
    if (cmAnalysis$stratifyByPs) {
      args <- cmAnalysis$stratifyByPsArgs
      args$population <- ps
      strataPop <- do.call(CohortMethod::stratifyByPs, args)
    } else if (cmAnalysis$matchOnPs) {
      args <- cmAnalysis$matchOnPsArgs
      args$population <- ps
      strataPop <- do.call(CohortMethod::matchOnPs, args)
    } else {
      strataPop <- ps
      strataPop$stratumId <- 0
    }
    if (nrow(strataPop) > 0) {
      cohortMethodDataFile <- file.path(cmOutputFolder, rows$cohortMethodDataFile[1])
      cohortMethodData <- CohortMethod::loadCohortMethodData(cohortMethodDataFile[1])
      balance <- CohortMethod::computeCovariateBalance(population = strataPop,
                                                       cohortMethodData = cohortMethodData)
      saveRDS(balance, outputFileName)
    }
  }

  # Per TCOA: compute balance only for select covariates ---------------------------------------------
  hois <- rows[rows$outcomeId %in% getOutcomesOfInterest(), ]
  hois$outputFileName <- file.path(balanceFolder, sprintf("bal_t%d_c%d_o%d_a%d.rds",
                                                          hois$targetId,
                                                          hois$comparatorId,
                                                          hois$outcomeId,
                                                          hois$analysisId))
  hois <- hois[!file.exists(hois$outputFileName), ]
  if (nrow(hois) > 0) {
    if (is.null(cohortMethodData)) {
      cohortMethodDataFile <- file.path(cmOutputFolder, rows$cohortMethodDataFile[1])
      cohortMethodData <- CohortMethod::loadCohortMethodData(cohortMethodDataFile[1])
    }
    table1Specs <- CohortMethod::getDefaultCmTable1Specifications()
    covariateAnalysisIds <- table1Specs$analysisId[is.na(table1Specs$covariateIds)]
    covariateIds <- cohortMethodData$covariateRef %>%
      filter(.data$analysisId %in% covariateAnalysisIds) %>%
      pull(.data$covariateId)
    for (i in 1:nrow(table1Specs)) {
      if (!is.na(table1Specs$covariateIds[i])) {
        covariateIds <- c(covariateIds, as.numeric(strsplit(table1Specs$covariateIds[i], ",")[[1]]))
      }
    }
    cohortMethodData$covariates <- cohortMethodData$covariates %>%
      filter(.data$covariateId %in% covariateIds)

    cohortMethodData$covariateRef <- cohortMethodData$covariateRef %>%
      filter(.data$covariateId %in% covariateIds)

    for (i in 1:nrow(hois)) {
      ParallelLogger::logTrace("Creating covariate balance file ", outputFileName)
      strataPop <- readRDS(file.path(cmOutputFolder, hois$strataFile[i]))
      if (nrow(strataPop) > 0) {
        balance <- CohortMethod::computeCovariateBalance(population = strataPop,
                                                         cohortMethodData = cohortMethodData)
        saveRDS(balance, hois$outputFileName[i])
      }
    }
  }
}

