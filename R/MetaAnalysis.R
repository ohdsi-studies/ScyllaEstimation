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


#' Title
#'
#' @param allDbsFolder   Folder on the local file system containing the results zip files of all data sites.
#' @param maExportFolder A local folder where the meta-anlysis results will be written.
#' @param maxCores       Maximum number of CPU cores to be used when computing the meta-analyses.
#'
#' @return
#' Does not return a value, but creates a new zip file in the \code{maExportFolder} for the meta-analyses.
#'
#' @export
synthesizeResults <- function(allDbsFolder, maExportFolder, maxCores) {
  # library(dplyr)
  if (file.exists(maExportFolder)) {
    dir.create(maExportFolder, recursive = TRUE)
  }

  zipFiles <- list.files(allDbsFolder, "*zip")
  mainResults <- lapply(zipFiles, loadMainResults, allDbsFolder = allDbsFolder)
  mainResults <- do.call(rbind, mainResults)
  mainResults <- split(mainResults, paste(mainResults$targetId, mainResults$comparatorId, mainResults$analysisId))
  profiles <- lapply(zipFiles, loadLikelihoodProfiles, allDbsFolder = allDbsFolder)
  profiles <- do.call(rbind, profiles)
  profiles <- split(profiles, paste(profiles$targetId, profiles$comparatorId, profiles$analysisId))

  combined <- lapply(names(mainResults), function(name) list(mainResults = mainResults[[name]], profiles = profiles[[name]]))

  rm(mainResults)
  rm(profiles)
  ParallelLogger::logInfo("Performing cross-database evidence synthesis")
  cluster <- ParallelLogger::makeCluster(min(maxCores, 10))
  results <- ParallelLogger::clusterApply(cluster, combined, computeGroupMetaAnalysis)
  ParallelLogger::stopCluster(cluster)
  results <- do.call(rbind, results)
  results$trueEffectSize <- NULL

  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  fileName <-  file.path(maExportFolder, paste0("cohort_method_result.csv"))
  write.csv(results, fileName, row.names = FALSE)

  ParallelLogger::logInfo("Creating database table")
  databases <- lapply(zipFiles, loadDatabase, allDbsFolder = allDbsFolder)
  databases <- do.call(rbind, databases)
  database <- data.frame(database_id = "Meta-analysis",
                         database_name = "Random effects meta-analysis",
                         description = "Random effects meta-analysis using non-normal likelihood approximation to avoid bias due to small and zero counts.",
                         vocabularyVersion = "",
                         minObsPeriodDate = min(databases$minobsperioddate),
                         maxObsPeriodDate = max(databases$maxobsperioddate),
                         studyPackageVersion = utils::packageVersion("ScyllaEstimation"),
                         is_meta_analysis = 1)
  fileName <- file.path(maExportFolder, "database.csv")
  write.csv(database, fileName, row.names = FALSE)

  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(maExportFolder, sprintf("Results_%s.zip", "MetaAnalysis"))
  files <- list.files(maExportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(maExportFolder)
  on.exit(setwd(oldWd))
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
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
  results <- readr::read_csv(file.path(tempFolder, "cohort_method_result.csv"), col_types = readr::cols(), guess_max = 1e5)
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

loadDatabase <- function(zipFile, allDbsFolder) {
  ParallelLogger::logInfo("Loading database information from ", zipFile)
  tempFolder <- tempfile()
  dir.create(tempFolder)
  on.exit(unlink(tempFolder, recursive = TRUE, force = TRUE))
  utils::unzip(zipfile = file.path(allDbsFolder, zipFile),
               files = c("database.csv"),
               exdir = tempFolder)
  profiles <- readr::read_csv(file.path(tempFolder, "database.csv"), col_types = readr::cols(), guess_max = 1e3)
  colnames(profiles) <- SqlRender::snakeCaseToCamelCase(colnames(profiles))
  return(profiles)
}

computeGroupMetaAnalysis <- function(group) {
  # Find an example where more than one DB has data:
  # for (i in 1:length(combined)) {
  #   group = combined[[i]]
  #   profiles <- group$profiles
  #   dbsPerOutcome <- aggregate(databaseId ~ outcomeId, profiles, length)
  #   if (any(dbsPerOutcome$databaseId > 1)) {
  #     print(paste("i:", i, ", outcomeIds:", paste(dbsPerOutcome$outcomeId[dbsPerOutcome$databaseId > 1], collapse = ", ")))
  #     break
  #   }
  # }
  # group = combined[[145]]
  mainResults <- group$mainResults
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
  # View(groupResults[, c("logRr", "seLogRr", "traditionalLogRr", "traditionalSeLogRr")])
  ncs <- groupResults[groupResults$trueEffectSize == 1, ]
  validNcs <- ncs[!is.na(ncs$seLogRr), ]
  if (nrow(validNcs) >= 5) {
    null <- EmpiricalCalibration::fitMcmcNull(validNcs$logRr, validNcs$seLogRr)
    calibratedP <- EmpiricalCalibration::calibrateP(null = null,
                                                    logRr = groupResults$logRr,
                                                    seLogRr = groupResults$seLogRr)
    groupResults$calibratedP <- calibratedP$p

    model <- EmpiricalCalibration::convertNullToErrorModel(null)
    calibratedCi <- EmpiricalCalibration::calibrateConfidenceInterval(logRr = groupResults$logRr,
                                                                      seLogRr = groupResults$seLogRr,
                                                                      model = model)
    groupResults$calibratedRr <- exp(calibratedCi$logRr)
    groupResults$calibratedCi95Lb <- exp(calibratedCi$logLb95Rr)
    groupResults$calibratedCi95Ub <- exp(calibratedCi$logUb95Rr)
    groupResults$calibratedLogRr <- calibratedCi$logRr
    groupResults$calibratedSeLogRr <- calibratedCi$seLogRr
  } else {
    groupResults$calibratedP <- rep(NA, nrow(groupResults))
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
  # outcomeId <- 152
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
  maRow$i2 <- NULL

  if (length(profileDbs) <= 1) {
    if (length(profileDbs) == 1) {
      idx <- (rows$databaseId == profileDbs)
    } else {
      idx <- 1
    }
    maRow$rr <- rows$rr[idx]
    maRow$ci95Lb <- rows$ci95Lb[idx]
    maRow$ci95Ub <- rows$ci95Ub[idx]
    maRow$p <- rows$p[idx]
    maRow$logRr <- rows$logRr[idx]
    maRow$seLogRr <- rows$seLogRr[idx]
    maRow$tau <- 0
    maRow$traditionalLogRr <- rows$logRr[idx]
    maRow$traditionalSeLogRr <- rows$seLogRr[idx]
  } else {
    profiles <- group$profiles$profile[group$profiles$outcomeId == outcomeId]
    profiles <- strsplit(profiles, ";")
    profiles <- as.data.frame(t(sapply(profiles, as.numeric)))
    colnames(profiles) <- seq(log(0.1), log(10), length.out = 1000)
    estimate <- EvidenceSynthesis::computeBayesianMetaAnalysis(profiles)
    # EvidenceSynthesis::plotPosterior(estimate)
    maRow$rr <- exp(estimate$mu)
    maRow$ci95Lb <- exp(estimate$mu95Lb)
    maRow$ci95Ub <- exp(estimate$mu95Ub)
    # TODO: some p-value calibration that does not assume normality?
    maRow$p <- EmpiricalCalibration::computeTraditionalP(estimate$logRr, estimate$seLogRr)
    maRow$logRr <- estimate$logRr
    maRow$seLogRr <- estimate$seLogRr
    maRow$tau <- estimate$tau

    # Adding traditional meta-analytic estimate for comparison:
    meta <- meta::metagen(TE = rows$logRr,
                          seTE = rows$seLogRr,
                          sm = "RR",
                          hakn = FALSE)
    s <- summary(meta)
    rnd <- s$random
    maRow$traditionalLogRr <- rnd$TE
    maRow$traditionalSeLogRr <- rnd$seTE
  }

  return(maRow)
}
