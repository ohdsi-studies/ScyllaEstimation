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

#' Create the exposure and outcome cohorts
#'
#' @details
#' This function will create the target, subgroup, and outcome cohorts using inputs
#' from this study package and using functions from ScyllaCharacterization
#'
#' @export
createCohorts <- function(connectionDetails,
                          connection = NULL,
                          cdmDatabaseSchema,
                          oracleTempSchema = NULL,
                          cohortDatabaseSchema,
                          cohortTable = "cohort",
                          cohortIdsToExcludeFromExecution = c(1100, 1101, 1102, 1103, 1104, 1105, 1106, # exposure classes (e.g. antivirals class)
                                                              2001, # subgroup where covid19+ before hosp w/o required 1y prior obs
                                                              2005, 2006), # subgroup where covid19+ after intensive services
                          cohortGroups = getUserSelectableCohortGroups(),
                          minCellCount = 0,
                          incremental = TRUE,
                          outputFolder,
                          incrementalFolder = file.path(outputFolder, "RecordKeeping")) {

  start <- Sys.time()

  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }

  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }

  if (!is.null(getOption("andromedaTempFolder")) && !file.exists(getOption("andromedaTempFolder"))) {
    warning("andromedaTempFolder '", getOption("andromedaTempFolder"), "' not found. Attempting to create folder")
    dir.create(getOption("andromedaTempFolder"), recursive = TRUE)
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Instantiate cohorts --------------------------------------------------------

  cohorts <- getCohortsToCreate()
  cohorts <- cohorts[!(cohorts$cohortId %in% cohortIdsToExcludeFromExecution), ] # Remove cohorts to be excluded
  targetCohortIds <- cohorts[cohorts$cohortType %in% cohortGroups, "cohortId"][[1]]
  subgroupCohortIds <- cohorts[cohorts$cohortType == "subgroup", "cohortId"][[1]]
  featureCohorts <- loadFile(c("OutcomeCohorts.csv", "ScyllaEstimation")) # use outcomes from OutcomeCohorts.csv, not full outcome set from ScyllaCharacterization
  featureCohortIds <- unique(featureCohorts$cohortId)

  # for dev ---
  targetCohortIds <- targetCohortIds[1:10]
  featureCohortIds <- featureCohortIds[1:10]

  if (length(targetCohortIds) > 0) {
    ParallelLogger::logInfo(" ---- Creating target cohorts ---- ")
    instantiateCohortSet(connectionDetails = connectionDetails,
                         connection = connection,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         oracleTempSchema = oracleTempSchema,
                         cohortDatabaseSchema = cohortDatabaseSchema,
                         cohortTable = cohortTable,
                         cohortIds = targetCohortIds,
                         createCohortTable = TRUE,
                         generateInclusionStats = FALSE,
                         incremental = incremental,
                         incrementalFolder = incrementalFolder,
                         inclusionStatisticsFolder = outputFolder)
  }

  if (length(subgroupCohortIds) > 0) {
    ParallelLogger::logInfo(" ---- Creating subgroup cohorts ---- ")
    instantiateCohortSet(connectionDetails = connectionDetails,
                         connection = connection,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         oracleTempSchema = oracleTempSchema,
                         cohortDatabaseSchema = cohortDatabaseSchema,
                         cohortTable = cohortTable,
                         cohortIds = subgroupCohortIds,
                         createCohortTable = FALSE,
                         generateInclusionStats = FALSE,
                         incremental = incremental,
                         incrementalFolder = incrementalFolder,
                         inclusionStatisticsFolder = outputFolder)
  }

  if (length(featureCohortIds) > 0) {
    ParallelLogger::logInfo(" ---- Creating outcome cohorts ---- ")
    instantiateCohortSet(connectionDetails = connectionDetails,
                         connection = connection,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         oracleTempSchema = oracleTempSchema,
                         cohortDatabaseSchema = cohortDatabaseSchema,
                         cohortTable = cohortTable,
                         cohortIds = featureCohortIds,
                         createCohortTable = FALSE,
                         generateInclusionStats = FALSE,
                         incremental = incremental,
                         incrementalFolder = incrementalFolder,
                         inclusionStatisticsFolder = outputFolder)
  }

  # Create the subgrouped cohorts
  ParallelLogger::logInfo(" ---- Creating subgrouped target cohorts ---- ")
  createBulkSubgroupFromFile(connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             cohortDatabaseSchema = cohortDatabaseSchema,
                             cohortStagingTable = cohortTable,
                             targetIds = targetCohortIds,
                             oracleTempSchema = oracleTempSchema)

  # Cohort counts --------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohorts")
  counts <- getCohortCounts(connection = connection,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            cohortTable = cohortTable)
  if (nrow(counts) > 0) {
    counts$databaseId <- databaseId
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  targetSubgroupXref <- getTargetSubgroupXref()
  targetSubgroupCohortRef <- targetSubgroupXref[targetSubgroupXref$targetId %in% targetCohortIds & targetSubgroupXref$subgroupId %in% c(1, 2, 3, 2002), c("cohortId", "name")]
  featureCohortRef <- unique(featureCohorts[, c("cohortId", "name")])
  cohortRef <- rbind(targetSubgroupCohortRef, featureCohortRef)
  counts <- dplyr::left_join(x = cohortRef, y = counts, by = "cohortId")
  writeToCsv(counts, file.path(outputFolder, "cohort_count.csv"), incremental = incremental, cohortId = counts$cohortId)
}




