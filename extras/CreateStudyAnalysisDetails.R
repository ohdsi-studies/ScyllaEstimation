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
  covarSettings <- FeatureExtraction::createDefaultCovariateSettings(addDescendantsToExclude = TRUE)

  getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(washoutPeriod = 183,
                                                                   restrictToCommonPeriod = FALSE,
                                                                   firstExposureOnly = TRUE,
                                                                   removeDuplicateSubjects = "remove all",
                                                                   studyStartDate = "",
                                                                   studyEndDate = "",
                                                                   excludeDrugsFromCovariates = FALSE,
                                                                   covariateSettings = covarSettings)

  createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                      minDaysAtRisk = 1,
                                                                      riskWindowStart = 0,
                                                                      addExposureDaysToStart = FALSE,
                                                                      riskWindowEnd = 30,
                                                                      addExposureDaysToEnd = TRUE)

  fitOutcomeModelArgs1 <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                  modelType = "cox",
                                                                  stratified = FALSE)

  cmAnalysis1 <- CohortMethod::createCmAnalysis(analysisId = 1,
                                                description = "No matching",
                                                getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                createStudyPopArgs = createStudyPopArgs,
                                                fitOutcomeModel = TRUE,
                                                fitOutcomeModelArgs = fitOutcomeModelArgs1)

  createPsArgs <- CohortMethod::createCreatePsArgs(control = Cyclops::createControl(cvType = "auto",
                                                                                    startingVariance = 0.01,
                                                                                    noiseLevel = "quiet",
                                                                                    tolerance = 2e-07,
                                                                                    cvRepetitions = 10))

  matchOnPsArgs1 <- CohortMethod::createMatchOnPsArgs(maxRatio = 1)

  fitOutcomeModelArgs2 <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                  modelType = "cox",
                                                                  stratified = TRUE)

  cmAnalysis2 <- CohortMethod::createCmAnalysis(analysisId = 2,
                                                description = "One-on-one matching",
                                                getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                createStudyPopArgs = createStudyPopArgs,
                                                createPs = TRUE,
                                                createPsArgs = createPsArgs,
                                                matchOnPs = TRUE,
                                                matchOnPsArgs = matchOnPsArgs1,
                                                fitOutcomeModel = TRUE,
                                                fitOutcomeModelArgs = fitOutcomeModelArgs2)

  matchOnPsArgs2 <- CohortMethod::createMatchOnPsArgs(maxRatio = 100)

  cmAnalysis3 <- CohortMethod::createCmAnalysis(analysisId = 3,
                                                description = "Variable ratio matching",
                                                getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                createStudyPopArgs = createStudyPopArgs,
                                                createPs = TRUE,
                                                createPsArgs = createPsArgs,
                                                matchOnPs = TRUE,
                                                matchOnPsArgs = matchOnPsArgs2,
                                                fitOutcomeModel = TRUE,
                                                fitOutcomeModelArgs = fitOutcomeModelArgs2)

  stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(numberOfStrata = 5)

  cmAnalysis4 <- CohortMethod::createCmAnalysis(analysisId = 4,
                                                description = "Stratification",
                                                getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                createStudyPopArgs = createStudyPopArgs,
                                                createPs = TRUE,
                                                createPsArgs = createPsArgs,
                                                stratifyByPs = TRUE,
                                                stratifyByPsArgs = stratifyByPsArgs,
                                                fitOutcomeModel = TRUE,
                                                fitOutcomeModelArgs = fitOutcomeModelArgs2)

  interactionCovariateIds <- c(8532001,
                               201826210,
                               21600960413)  # Female, T2DM, concurent use of antithrombotic agents

  fitOutcomeModelArgs3 <- CohortMethod::createFitOutcomeModelArgs(modelType = "cox",
                                                                  stratified = TRUE,
                                                                  useCovariates = FALSE,
                                                                  interactionCovariateIds = interactionCovariateIds)

  cmAnalysis5 <- CohortMethod::createCmAnalysis(analysisId = 5,
                                                description = "Stratification with interaction terms",
                                                getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                createStudyPopArgs = createStudyPopArgs,
                                                createPs = TRUE,
                                                createPsArgs = createPsArgs,
                                                stratifyByPs = TRUE,
                                                stratifyByPsArgs = stratifyByPsArgs,
                                                fitOutcomeModel = TRUE,
                                                fitOutcomeModelArgs = fitOutcomeModelArgs3)

  cmAnalysisList <- list(cmAnalysis1, cmAnalysis2, cmAnalysis3, cmAnalysis4, cmAnalysis5)

  CohortMethod::saveCmAnalysisList(cmAnalysisList, file.path(workFolder, "cmAnalysisList.json"))
}

createExposureConceptSet <- function(workFolder) {
  exposureCohortRefFile <- system.file("settings", "CohortsToCreateTarget.csv", package = "ScyllaCharacterization")
  exposureCohortRef <- read.csv(exposureCohortRefFile)
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
  write.csv(exposureConcepts, file.path(workFolder, "TargetCohortConceptIds.csv"), row.names = FALSE)
}

createTcoDetails <- function(workFolder, exposureGroupCohortIds = c(1100, 1101, 1102, 1103, 1104, 1105, 1106)) {
  fileInputs <- list(targetCohortCategories = c("TargetCohortCategories.csv", "ScyllaEstimation"),
                     targetCohortConceptIds = c("TargetCohortConceptIds.csv", "ScyllaEstimation"),
                     targetSubgroupXref = c("targetSubgroupXref.csv", "ScyllaCharacterization"),
                     outcomeCohorts = c("OutcomeCohorts.csv", "ScyllaEstimation"))

  loadFile <- function(fileInput) {
    fileName <- system.file("settings", fileInput[1], package = fileInput[2])
    df <-  read.csv(fileName)
    return(df)
  }

  dfs <- lapply(fileInputs, loadFile)
  list2env(dfs, envir = .GlobalEnv)

  targetCohortCategories$name <- NULL # note 2x azithromycin 1007 (classified as AB and AV)
  targetSubgroupXref <- targetSubgroupXref[targetSubgroupXref$subgroupId %in% c(2002, 2004), ] # keep cohorts in scope for estimation with >=365d prior observation
  targetSubgroupXref <- targetSubgroupXref[!(targetSubgroupXref$targetId %in% exposureGroupCohortIds), ] # drops exposure group cohorts (e.g. antivirals class)
  targetSubgroupXref <- merge(targetSubgroupXref, targetCohortConceptIds, by.x = "targetId", by.y = "cohortId")
  targetSubgroupXref <- merge(targetSubgroupXref, targetCohortCategories)

  categories <- split(targetSubgroupXref, paste(targetSubgroupXref$targetCategoryId, targetSubgroupXref$subgroupId))

  createTcosByCategoryAndSubgroup <- function(category) { # category <- categories[[9]]
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


createNegativeContolDetails <- function(workFolder) {

}

createPositiveControlSynthesisArgs <- function(workFolder) {
  settings <- list(outputIdOffset = 10000,
                   firstExposureOnly = TRUE,
                   firstOutcomeOnly = TRUE,
                   removePeopleWithPriorOutcomes = TRUE,
                   modelType = "survival",
                   washoutPeriod = 183,
                   riskWindowStart = 0,
                   riskWindowEnd = 30,
                   addExposureDaysToEnd = TRUE,
                   effectSizes = c(1.5, 2, 4),
                   precision = 0.01,
                   prior = Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                   control = Cyclops::createControl(cvType = "auto",
                                                    startingVariance = 0.01,
                                                    noiseLevel = "quiet",
                                                    cvRepetitions = 1,
                                                    threads = 1),
                   maxSubjectsForModel = 250000,
                   minOutcomeCountForModel = 50,
                   minOutcomeCountForInjection = 25,
                   covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                                  useDemographicsGender = TRUE,
                                                                                  useDemographicsIndexYear = TRUE,
                                                                                  useDemographicsIndexMonth = TRUE,
                                                                                  useConditionGroupEraLongTerm = TRUE,
                                                                                  useDrugGroupEraLongTerm = TRUE,
                                                                                  useProcedureOccurrenceLongTerm = TRUE,
                                                                                  useMeasurementLongTerm = TRUE,
                                                                                  useObservationLongTerm = TRUE,
                                                                                  useCharlsonIndex = TRUE,
                                                                                  useDcsi = TRUE,
                                                                                  useChads2Vasc = TRUE,
                                                                                  longTermStartDays = -365,
                                                                                  endDays = 0))
  ParallelLogger::saveSettingsToJson(settings,
                                     file.path(workFolder, "positiveControlSynthArgs.json"))
}

