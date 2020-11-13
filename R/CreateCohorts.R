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
                          cohortGroups = ScyllaCharacterization::getUserSelectableCohortGroups(),
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

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Instantiate cohorts --------------------------------------------------------

  cohorts <- ScyllaCharacterization::getCohortsToCreate()
  cohorts <- cohorts[!(cohorts$cohortId %in% cohortIdsToExcludeFromExecution), ] # Remove cohorts to be excluded
  targetCohortIds <- cohorts[cohorts$cohortType %in% cohortGroups, "cohortId"][[1]]
  subgroupCohortIds <- cohorts[cohorts$cohortType == "subgroup", "cohortId"][[1]]
  featureCohorts <- ScyllaCharacterization::readCsv("settings/OutcomeCohorts.csv", "ScyllaEstimation")
  featureCohortIds <- unique(featureCohorts$cohortId)

  # for dev ---
  # targetCohortIds <- targetCohortIds[1:13]
  # featureCohortIds <- featureCohortIds[1:13]

  if (length(targetCohortIds) > 0) {
    ParallelLogger::logInfo(" ---- Creating target cohorts ---- ")
    ScyllaCharacterization::instantiateCohortSet(connectionDetails = connectionDetails,
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
    ScyllaCharacterization::instantiateCohortSet(connectionDetails = connectionDetails,
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
    ScyllaCharacterization::instantiateCohortSet(connectionDetails = connectionDetails,
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
  ScyllaCharacterization::createBulkSubgroupFromFile(connection = connection,
                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                                     cohortStagingTable = cohortTable,
                                                     targetIds = targetCohortIds,
                                                     oracleTempSchema = oracleTempSchema)

  # Create negative control outcomes
  ParallelLogger::logInfo(" ---- Creating negative control outcome cohorts ---- ")
  negativeControls <- getNegativeControlOutcomes()
  sql <- SqlRender::loadRenderTranslateSql("NegativeControlOutcomes.sql",
                                           "ScyllaEstimation",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           target_database_schema = cohortDatabaseSchema,
                                           target_cohort_table = cohortTable,
                                           outcome_ids = negativeControls$outcomeId)
  DatabaseConnector::executeSql(connection, sql)

  # Cohort counts --------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohorts")
  counts <- ScyllaCharacterization::getCohortCounts(connection = connection,
                                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                                    cohortTable = cohortTable)
  if (nrow(counts) > 0) {
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  targetSubgroupXref <- ScyllaCharacterization::getTargetSubgroupXref()
  targetSubgroupCohortRef <- targetSubgroupXref[targetSubgroupXref$targetId %in% targetCohortIds & targetSubgroupXref$subgroupId %in% c(1, 2, 3, 2002), c("cohortId", "name")]
  featureCohortRef <- unique(featureCohorts[, c("cohortId", "name")])
  cohortRef <- rbind(targetSubgroupCohortRef, featureCohortRef)
  counts <- dplyr::left_join(x = cohortRef, y = counts, by = "cohortId")
  counts$databaseId <- databaseId
  ScyllaCharacterization::writeToCsv(counts, file.path(outputFolder, "cohort_count.csv"), incremental = incremental, cohortId = counts$cohortId)
}
