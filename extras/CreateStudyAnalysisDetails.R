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

createAnalysesDetails <- function(workFolder) {

  createScyllaCovariateSettings <- function(endDays) {
    covarSettings <- FeatureExtraction::createDefaultCovariateSettings(addDescendantsToExclude = TRUE)
    covarSettings$endDays <- endDays
    return(covarSettings)
  }

  # TODO Do we need to subgroup analysisIds by exposure cohorts?
  #
  # - 100+: Washout, treatment on date of admission and prior to intensive services, -1 endDays
  # - 200+: No washout, treatment on date of admission and prior to intensive services, 0 endDays
  # - 300+: Washout, treatment during hospitalization, -1 endDays
  # - 400+: Washout, after COVID+ test, 0 endDays

  getDbCmDataArgsZeroDays <-
    CohortMethod::createGetDbCohortMethodDataArgs(studyStartDate = "20200101",
                                                  studyEndDate = "",
                                                  restrictToCommonPeriod = TRUE,
                                                  covariateSettings = createScyllaCovariateSettings(endDays = 0))

  getDbCmDataArgsMinusOneDays <-
    CohortMethod::createGetDbCohortMethodDataArgs(studyStartDate = "20200101",
                                                  studyEndDate = "",
                                                  restrictToCommonPeriod = TRUE,
                                                  covariateSettings = createScyllaCovariateSettings(endDays = -1))

  createScyllaStudyPopulation <- function(washoutPeriod, riskWindowEnd, endAnchor) {
    CohortMethod::createCreateStudyPopulationArgs(washoutPeriod = washoutPeriod,
                                                  removeDuplicateSubjects = "keep first",
                                                  removeSubjectsWithPriorOutcome = TRUE,
                                                  minDaysAtRisk = 1,
                                                  riskWindowStart = 1,
                                                  endAnchor = endAnchor,
                                                  riskWindowEnd = riskWindowEnd)
  }

  fitScyllaOutcomeModelArgs <- function(modelType, stratified) {
    args <- CohortMethod::createFitOutcomeModelArgs(modelType = modelType,
                                                    stratified = stratified)
    return(args)
  }

  fitOutcomeModelArgsLogisticNoStrata <- fitScyllaOutcomeModelArgs(modelType = "logistic", stratified = FALSE)

  fitOutcomeModelArgsCoxNoStrata <- fitScyllaOutcomeModelArgs(modelType = "cox", stratified = FALSE)

  fitOutcomeModelArgsLogisticWithStrata <- fitScyllaOutcomeModelArgs(modelType = "logistic", stratified = TRUE)

  fitOutcomeModelArgsCoxWithStrata <- fitScyllaOutcomeModelArgs(modelType = "cox", stratified = TRUE)

  createPsArgs <- CohortMethod::createCreatePsArgs(stopOnError = FALSE,
                                                   control = Cyclops::createControl(noiseLevel = "silent",
                                                                                    cvType = "auto",
                                                                                    seed = 1,
                                                                                    tolerance = 2e-07,
                                                                                    fold = 3,
                                                                                    cvRepetitions = 10,
                                                                                    startingVariance = 0.01))

  matchOnPsOneToOneArgs <- CohortMethod::createMatchOnPsArgs(maxRatio = 1)

  matchOnPsOneToManyArgs <- CohortMethod::createMatchOnPsArgs(maxRatio = 100,
                                                              allowReverseMatch = TRUE)

  stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(numberOfStrata = 5)

  createCrudeScyllaCmAnalysis <- function(analysisId, description, getDbCohortMethodDataArgs, createStudyPopArgs, fitOutcomeModelArgs) {
    CohortMethod::createCmAnalysis(analysisId = analysisId,
                                   description = description,
                                   getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
                                   createStudyPopArgs = createStudyPopArgs,
                                   fitOutcomeModel = TRUE,
                                   fitOutcomeModelArgs = fitOutcomeModelArgs)
  }

  createOneToOneMatchScyllaCmAnalysis <- function(analysisId, description, getDbCohortMethodDataArgs, createStudyPopArgs, fitOutcomeModelArgs) {
    CohortMethod::createCmAnalysis(analysisId = analysisId,
                                   description = description,
                                   getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
                                   createStudyPopArgs = createStudyPopArgs,
                                   createPs = TRUE,
                                   createPsArgs = createPsArgs,
                                   matchOnPs = TRUE,
                                   matchOnPsArgs = matchOnPsOneToOneArgs,
                                   fitOutcomeModel = TRUE,
                                   fitOutcomeModelArgs = fitOutcomeModelArgs)
  }

  createOneToManyMatchScyllaCmAnalysis <- function(analysisId, description, getDbCohortMethodDataArgs, createStudyPopArgs, fitOutcomeModelArgs) {
    CohortMethod::createCmAnalysis(analysisId = analysisId,
                                   description = description,
                                   getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
                                   createStudyPopArgs = createStudyPopArgs,
                                   createPs = TRUE,
                                   createPsArgs = createPsArgs,
                                   matchOnPs = TRUE,
                                   matchOnPsArgs = matchOnPsOneToManyArgs,
                                   fitOutcomeModel = TRUE,
                                   fitOutcomeModelArgs = fitOutcomeModelArgs)
  }

  createStratifiedScyllaCmAnalysis <- function(analysisId, description, getDbCohortMethodDataArgs, createStudyPopArgs, fitOutcomeModelArgs) {
    CohortMethod::createCmAnalysis(analysisId = analysisId,
                                   description = description,
                                   getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
                                   createStudyPopArgs = createStudyPopArgs,
                                   createPs = TRUE,
                                   createPsArgs = createPsArgs,
                                   stratifyByPs = TRUE,
                                   stratifyByPsArgs = stratifyByPsArgs,
                                   fitOutcomeModel = TRUE,
                                   fitOutcomeModelArgs = fitOutcomeModelArgs)
  }

  createSetOfScyllaCmAnalysis <- function(startId, descriptionStub, washoutPeriod,
                                          getDbCmDataArgs,
                                          fitLogisticOutcomeModelArgs,
                                          fitCoxOutcomeModelArgs,
                                          functor) {
    list(
      functor(analysisId = startId + 0,
              description = paste(descriptionStub, "7 days; logistic"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort start",
                                                               riskWindowEnd = 7),
              fitOutcomeModelArgs = fitLogisticOutcomeModelArgs),

      functor(analysisId = startId + 1,
              description = paste(descriptionStub, "30 days; logistic"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort start",
                                                               riskWindowEnd = 30),
              fitOutcomeModelArgs = fitLogisticOutcomeModelArgs),

      functor(analysisId = startId + 2,
              description = paste(descriptionStub, "on treatment; logistic"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort end",
                                                               riskWindowEnd = 0),
              fitOutcomeModelArgs = fitLogisticOutcomeModelArgs),

      functor(analysisId = startId + 3,
              description = paste(descriptionStub, "7 days; cox"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort start",
                                                               riskWindowEnd = 7),
              fitOutcomeModelArgs = fitCoxOutcomeModelArgs),

      functor(analysisId = startId + 4,
              description = paste(descriptionStub, "30 days; cox"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort start",
                                                               riskWindowEnd = 30),
              fitOutcomeModelArgs = fitCoxOutcomeModelArgs),

      functor(analysisId = startId + 5,
              description = paste(descriptionStub, "on treatment; cox"),
              getDbCohortMethodDataArgs = getDbCmDataArgs,
              createStudyPopArgs = createScyllaStudyPopulation(washoutPeriod = washoutPeriod,
                                                               endAnchor = "cohort end",
                                                               riskWindowEnd = 0),
              fitOutcomeModelArgs = fitCoxOutcomeModelArgs)
    )
  }

  createLargerSetOfScyllaCmAnalysis <- function(startId, descriptionStub, washoutPeriod, getDbCmDataArgs) {
    c(
      createSetOfScyllaCmAnalysis(startId = startId + 0,
                                  descriptionStub = paste(descriptionStub, "Crude;"),
                                  washoutPeriod = washoutPeriod,
                                  getDbCmDataArgs = getDbCmDataArgs,
                                  fitLogisticOutcomeModelArgs = fitOutcomeModelArgsLogisticNoStrata,
                                  fitCoxOutcomeModelArgs = fitOutcomeModelArgsCoxNoStrata,
                                  functor = createCrudeScyllaCmAnalysis),

      createSetOfScyllaCmAnalysis(startId = startId + 6,
                                  descriptionStub = paste(descriptionStub, "1-to-1 matched;"),
                                  washoutPeriod = washoutPeriod,
                                  getDbCmDataArgs = getDbCmDataArgs,
                                  fitLogisticOutcomeModelArgs = fitOutcomeModelArgsLogisticNoStrata,
                                  fitCoxOutcomeModelArgs = fitOutcomeModelArgsCoxNoStrata,
                                  functor = createOneToOneMatchScyllaCmAnalysis),

      createSetOfScyllaCmAnalysis(startId = startId + 12,
                                  descriptionStub = paste(descriptionStub, "1-to-many matched;"),
                                  washoutPeriod = washoutPeriod,
                                  getDbCmDataArgs = getDbCmDataArgs,
                                  fitLogisticOutcomeModelArgs = fitOutcomeModelArgsLogisticWithStrata,
                                  fitCoxOutcomeModelArgs = fitOutcomeModelArgsCoxWithStrata,
                                  functor = createOneToManyMatchScyllaCmAnalysis),

      createSetOfScyllaCmAnalysis(startId = startId + 18,
                                  descriptionStub = paste(descriptionStub, "Stratified;"),
                                  washoutPeriod = washoutPeriod,
                                  getDbCmDataArgs = getDbCmDataArgs,
                                  fitLogisticOutcomeModelArgs = fitOutcomeModelArgsLogisticWithStrata,
                                  fitCoxOutcomeModelArgs = fitOutcomeModelArgsCoxWithStrata,
                                  functor = createStratifiedScyllaCmAnalysis)
    )
  }

  cmAnalysisList <- c(
    createLargerSetOfScyllaCmAnalysis(startId = 101,
                                      descriptionStub = "Admission to intensive services; washout; -1 end days;",
                                      washoutPeriod = 365,
                                      getDbCmDataArgs = getDbCmDataArgsMinusOneDays),

    createLargerSetOfScyllaCmAnalysis(startId = 201,
                                      descriptionStub = "Admission to intensive services; no washout; 0 end days;",
                                      washoutPeriod = 0,
                                      getDbCmDataArgs = getDbCmDataArgsZeroDays),

    createLargerSetOfScyllaCmAnalysis(startId = 301,
                                      descriptionStub = "During hospitalization to intensive services; washout; -1 end days;",
                                      washoutPeriod = 365,
                                      getDbCmDataArgs = getDbCmDataArgsMinusOneDays),

    createLargerSetOfScyllaCmAnalysis(startId = 401,
                                      descriptionStub = "Post testing to hospitalization; washout; 0 end days;",
                                      washoutPeriod = 365,
                                      getDbCmDataArgs = getDbCmDataArgsZeroDays)
  )

  CohortMethod::saveCmAnalysisList(cmAnalysisList, file.path(workFolder, "cmAnalysisList.json"))
}

createExposureConceptSet <- function(workFolder) {
  exposureCohortRef <- ScyllaCharacterization::readCsv("settings/CohortsToCreateTarget.csv", "ScyllaCharacterization")
  rows <- split(exposureCohortRef, exposureCohortRef$cohortId)

  getExposureConcepts <- function(row) {
    cohortJsonFile <- system.file("cohorts", sprintf("%s.json", row$cohortId), package = "ScyllaCharacterization")
    cohortJson <- readLines(cohortJsonFile)
    cohortJson <- jsonlite::fromJSON(cohortJson)
    concepts <- tibble::tibble()
    for (i in 1:length(cohortJson$ConceptSets$expression$items)) {
      conceptRow <- cohortJson$ConceptSets$expression$items[[i]]$concept
      names(conceptRow) <- SqlRender::snakeCaseToCamelCase(names(conceptRow))
      conceptRow <- dplyr::bind_cols(row, conceptRow)
      concepts <- dplyr::bind_rows(concepts, conceptRow)
    }
    return(concepts)
  }

  exposureConcepts <- lapply(rows, getExposureConcepts)
  exposureConcepts <- dplyr::bind_rows(exposureConcepts)
  exposureConcepts <- exposureConcepts %>%
    dplyr::group_by(cohortId) %>%
    dplyr::summarise(conceptIds = paste(conceptId, collapse = ";"), .groups = "drop")

  additionalExcludedCovariates <- ScyllaCharacterization::readCsv("settings/AdditionalExcludedCovariateConceptIds.csv", "ScyllaEstimation")
  exposureConcepts <- dplyr::left_join(exposureConcepts, additionalExcludedCovariates, by = "cohortId")
  adds <- !is.na(exposureConcepts$additionalExcludedCovariateConceptIds)
  exposureConcepts$conceptIds[adds] <- paste(exposureConcepts$conceptIds[adds], exposureConcepts$additionalExcludedCovariateConceptIds[adds], sep = ";")
  exposureConcepts <- subset(exposureConcepts, select = -additionalExcludedCovariateConceptIds)

  write.csv(exposureConcepts, file.path(workFolder, "TargetCohortConceptIds.csv"), row.names = FALSE)
}

createTcoDetails <- function(workFolder,
                             exposureGroupCohortIds = c(1100, 1101, 1102, 1103, 1104, 1105, 1106)) {
  fileInputs <- list(targetCohortCategories = c("settings/TargetCohortCategories.csv", "ScyllaEstimation"),
                     targetCohortConceptIds = c("settings/TargetCohortConceptIds.csv", "ScyllaEstimation"),
                     targetSubgroupXref = c("settings/targetSubgroupXref.csv", "ScyllaCharacterization"),
                     outcomeCohorts = c("settings/OutcomeCohorts.csv", "ScyllaEstimation"))
  dfs <- lapply(fileInputs, function(x) return(ScyllaCharacterization::readCsv(x[1], x[2])))
  list2env(dfs, envir = .GlobalEnv)

  targetCohortCategories$name <- NULL # note 2x azithromycin 1007 (classified as AB and AV)
  targetSubgroupXref <- targetSubgroupXref[targetSubgroupXref$subgroupId %in% c(1, 2, 3, 2002), ] # keep subgroups in scope to estimation study
  targetSubgroupXref <- targetSubgroupXref[!(targetSubgroupXref$targetId %in% exposureGroupCohortIds), ] # drops exposure group cohorts (e.g. antivirals class)
  targetSubgroupXref <- merge(targetSubgroupXref, targetCohortConceptIds, by.x = "targetId", by.y = "cohortId")
  targetSubgroupXref <- merge(targetSubgroupXref, targetCohortCategories)

  categories <- split(targetSubgroupXref, paste(targetSubgroupXref$targetCategoryId, targetSubgroupXref$subgroupId))

  createTcosByCategoryAndSubgroup <- function(category) { # category <- categories[[1]]
    tcos <- data.frame(t(combn(category$cohortId, 2)))
    names(tcos) <- c("targetId", "comparatorId")
    outcomeIds <- outcomeCohorts$cohortId[outcomeCohorts$subgroupId == category$subgroupId[1]]
    tcos$outcomeIds <- rep(paste(outcomeIds, collapse = ";"), nrow(tcos))
    tcos <- merge(tcos, category[, c("cohortId", "conceptIds")], by.x = "targetId", by.y = "cohortId")
    names(tcos)[names(tcos) == "conceptIds"] <- "tExcludedCovariateConceptIds"
    tcos <- merge(tcos, category[, c("cohortId", "conceptIds")], by.x = "comparatorId", by.y = "cohortId")
    names(tcos)[names(tcos) == "conceptIds"] <- "cExcludedCovariateConceptIds"
    tcos$excludedCovariateConceptIds <- paste(tcos$tExcludedCovariateConceptIds, tcos$cExcludedCovariateConceptIds, sep = ";")
    tcos <- subset(tcos, select = -c(tExcludedCovariateConceptIds, cExcludedCovariateConceptIds))
    return(tcos)
  }

  tcos <- lapply(categories, createTcosByCategoryAndSubgroup)
  tcos <- dplyr::bind_rows(tcos)
  tcos <- tcos[, c("targetId", "comparatorId", "outcomeIds", "excludedCovariateConceptIds")]
  write.csv(tcos, file.path(workFolder, "TcosOfInterest.csv"), row.names = FALSE)
}
