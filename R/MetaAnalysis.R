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

# Unfinished. Unable to test due to change in grid between runs
synthesizeResults <- function(allDbsFolder, maxCores) {

  zipFiles <- list.files(allDbsFolder, "*zip")
  mainResults <- lapply(zipFiles, loadMainResults, allDbsFolder = allDbsFolder)
  mainResults <- bind_rows(mainResults)
  mainResults <- split(mainResults, paste(mainResults$targetId, mainResults$comparatorId, mainResults$analysisId))
  profiles <- lapply(zipFiles, loadLikelihoodProfiles, allDbsFolder = allDbsFolder)
  profiles <- bind_rows(profiles)
  profiles <- split(profiles, paste(profiles$targetId, profiles$comparatorId, profiles$analysisId))

  combined <- lapply(names(mainResults), function(name) list(mainResults = mainResults[[name]], profiles = profiles[[name]]))

  rm(mainResults)
  rm(profiles)
  cluster <- ParallelLogger::makeCluster(min(maxCores, 10))
  results <- ParallelLogger::clusterApply(cluster, combined, computeGroupMetaAnalysis)
  ParallelLogger::stopCluster(cluster)
  results <- do.call(rbind, results)

  results$trueEffectSize <- NULL

  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  fileName <-  file.path(allDbsFolder, paste0("cohort_method_result.csv"))
  write.csv(results, fileName, row.names = FALSE)

}

loadMainResults <- function(zipFile, allDbsFolder) {
  ParallelLogger::logInfo("Loading results from ", zipFile, " for evidence synthesis")
  tempFolder <- tempfile()
  dir.create(tempFolder)
  on.exit(unlink(tempFolder, recursive = TRUE, force = TRUE))
  utils::unzip(zipfile = file.path(allDbsFolder, zipFile),
               files = c("cohort_method_result.csv",
                         "negative_control_outcome.csv"),
               exdir = tempFolder)
  results <- readr::read_csv(file.path(tempFolder, "cohort_method_result.csv"), col_types = readr::cols(), guess_max = 1e4)
  colnames(results) <- SqlRender::snakeCaseToCamelCase(colnames(results))
  ncs <- readr::read_csv(file.path(tempFolder, "negative_control_outcome.csv"), col_types = readr::cols(), guess_max = 1e5)
  colnames(ncs) <- SqlRender::snakeCaseToCamelCase(colnames(ncs))
  results$trueEffectSize <- NA
  idx <- results$outcomeId %in% ncs$outcomeId
  results$trueEffectSize[idx] <- 1
  return(results)
}

loadLikelihoodProfiles <- function(zipFile, allDbsFolder) {
  ParallelLogger::logInfo("Loading likelihood profiles from ", zipFile, " for evidence synthesis")
  tempFolder <- tempfile()
  dir.create(tempFolder)
  on.exit(unlink(tempFolder, recursive = TRUE, force = TRUE))
  utils::unzip(zipfile = file.path(allDbsFolder, zipFile),
               files = c("likelihood_profile.csv"),
               exdir = tempFolder)
  profiles <- readr::read_csv(file.path(tempFolder, "likelihood_profile.csv"), col_types = readr::cols(), guess_max = 1e3)
  colnames(profiles) <- SqlRender::snakeCaseToCamelCase(colnames(profiles))
  return(profiles)
}

computeGroupMetaAnalysis <- function(group) {
  # group = combined[[3000]]
  mainResults <- group$mainResults
  # unique(mainResults$databaseId)
  if (nrow(mainResults) == 0) {
    return(NULL)
  }
  analysisId <- mainResults$analysisId[1]
  targetId <- mainResults$targetId[1]
  comparatorId <- mainResults$comparatorId[1]
  ParallelLogger::logTrace("Performing meta-analysis for target ", targetId, ", comparator ", comparatorId, ", analysis ", analysisId)
  outcomeIds <- unique(mainResults$outcomeId)
  outcomeGroupResults <- lapply(outcomeIds, computeSingleMetaAnalysis, group)
  groupResults <- do.call(rbind, outcomeGroupResults)
  ncs <- groupResults[groupResults$trueEffectSize == 1, ]
  validNcs <- ncs[!is.na(ncs$seLogRr), ]
  if (nrow(validNcs) >= 5) {
    null <- EmpiricalCalibration::fitMcmcNull(validNcs$logRr, validNcs$seLogRr)
    calibratedP <- EmpiricalCalibration::calibrateP(null = null,
                                                    logRr = groupResults$logRr,
                                                    seLogRr = groupResults$seLogRr)
    groupResults$calibratedP <- calibratedP$p
  } else {
    groupResults$calibratedP <- NA
  }

  if (nrow(validPcs) > 5) {
    model <- EmpiricalCalibration::fitSystematicErrorModel(logRr = c(validNcs$logRr, validPcs$logRr),
                                                           seLogRr = c(validNcs$seLogRr,
                                                                       validPcs$seLogRr),
                                                           trueLogRr = c(rep(0, nrow(validNcs)),
                                                                         log(validPcs$trueEffectSize)),
                                                           estimateCovarianceMatrix = FALSE)
    calibratedCi <- EmpiricalCalibration::calibrateConfidenceInterval(logRr = groupResults$logRr,
                                                                      seLogRr = groupResults$seLogRr,
                                                                      model = model)
    groupResults$calibratedRr <- exp(calibratedCi$logRr)
    groupResults$calibratedCi95Lb <- exp(calibratedCi$logLb95Rr)
    groupResults$calibratedCi95Ub <- exp(calibratedCi$logUb95Rr)
    groupResults$calibratedLogRr <- calibratedCi$logRr
    groupResults$calibratedSeLogRr <- calibratedCi$seLogRr
  } else {
    groupResults$calibratedRr <- rep(NA, nrow(groupResults))
    groupResults$calibratedCi95Lb <- rep(NA, nrow(groupResults))
    groupResults$calibratedCi95Ub <- rep(NA, nrow(groupResults))
    groupResults$calibratedLogRr <- rep(NA, nrow(groupResults))
    groupResults$calibratedSeLogRr <- rep(NA, nrow(groupResults))
  }
return(groupResults)
}

sumMinCellCount <- function(counts) {
  total <- sum(abs(counts))
  if (any(counts < 0)) {
    total <- -total
  }
  return(total)
}

computeSingleMetaAnalysis <- function(outcomeId, group) {
  # outcomeId <- group$mainResults$outcomeId[1]
  rows <- group$mainResults[group$mainResults$outcomeId == outcomeId, ]
  profileDbs <- if (is.null(group$profiles)) c() else group$profiles$databaseId[group$profiles$outcomeId == outcomeId]

  maRow <- rows[1, ]
  maRow$databaseId <- "Meta-analysis"
  maRow$targetSubjects <- sumMinCellCount(rows$targetSubjects)
  maRow$comparatorSubjects <- sumMinCellCount(rows$comparatorSubjects)
  maRow$targetDays <- sum(rows$targetDays)
  maRow$comparatorDays <- sum(rows$comparatorDays)
  maRow$targetOutcomes <- sumMinCellCount(rows$targetOutcomes)
  maRow$comparatorOutcomes <- sumMinCellCount(rows$comparatorOutcomes)

  if (length(profileDbs) == 1) {
    idx <- (rows$databaseId == profileDbs)
    maRow$rr <- rows$rr[idx]
    maRow$ci95Lb <- rows$ci95Lb[idx]
    maRow$ci95Ub <- rows$ci95Ub[idx]
    maRow$p <- rows$p[idx]
    maRow$logRr <- rows$logRr[idx]
    maRow$seLogRr <- rows$seLogRr[idx]
    maRow$tau <- 0
  } else if (length(profileDbs) > 1) {
    profiles <- group$profiles[group$profiles$outcomeId == outcomeId, ]
    profiles$databaseId <- NULL
    profiles$targetId <- NULL
    profiles$comparatorId <- NULL
    profiles$outcomeId <- NULL
    profiles$analysisId <- NULL
    estimate <- EvidenceSynthesis::computeBayesianMetaAnalysis(profiles)
    # plot(as.numeric(names(profiles)), profiles)
  }


  outcomeGroup <- outcomeGroup[!is.na(outcomeGroup$seLogRr), ]
  if (nrow(outcomeGroup) == 0) {
    maRow$targetSubjects <- 0
    maRow$comparatorSubjects <- 0
    maRow$targetDays <- 0
    maRow$comparatorDays <- 0
    maRow$targetOutcomes <- 0
    maRow$comparatorOutcomes <- 0
    maRow$rr <- NA
    maRow$ci95Lb <- NA
    maRow$ci95Ub <- NA
    maRow$p <- NA
    maRow$logRr <- NA
    maRow$seLogRr <- NA
    maRow$i2 <- NA
  } else if (nrow(outcomeGroup) == 1) {
    maRow <- outcomeGroup[1, ]
    maRow$i2 <- 0
  } else {
    maRow$targetSubjects <- sumMinCellCount(outcomeGroup$targetSubjects)
    maRow$comparatorSubjects <- sumMinCellCount(outcomeGroup$comparatorSubjects)
    maRow$targetDays <- sum(outcomeGroup$targetDays)
    maRow$comparatorDays <- sum(outcomeGroup$comparatorDays)
    maRow$targetOutcomes <- sumMinCellCount(outcomeGroup$targetOutcomes)
    maRow$comparatorOutcomes <- sumMinCellCount(outcomeGroup$comparatorOutcomes)
    meta <- meta::metagen(TE = outcomeGroup$logRr,
                          seTE = outcomeGroup$seLogRr,
                          sm = "RR",
                          hakn = FALSE)
    s <- summary(meta)
    maRow$i2 <- s$I2$TE
    rnd <- s$random
    maRow$rr <- exp(rnd$TE)
    maRow$ci95Lb <- exp(rnd$lower)
    maRow$ci95Ub <- exp(rnd$upper)
    maRow$p <- rnd$p
    maRow$logRr <- rnd$TE
    maRow$seLogRr <- rnd$seTE
  }
  maRow$databaseId <-
    return(maRow)
}
