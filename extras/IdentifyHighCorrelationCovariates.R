# Some code that is unfortunately non-reusable because custom temporal covariates were generated in Scylla characterization:
library(DatabaseConnector)
library(dplyr)

workingFolder <- "s:/ScyllaEstimation"

# Get high-correlation covariates from Scylla Characerization server ----------------------------
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("scyllaServer"),
                                                            keyring::key_get("scyllaDatabase"), sep = "/"),
                                             user = keyring::key_get("scyllaUser"),
                                             password = keyring::key_get("scyllaPassword"))
connection <- connect(connectionDetails)
sql <- SqlRender::readSql("extras/GetCovariates.sql")

# sql <- "SELECT * FROM scylla.cohort WHERE cohort_id = 1007000011;"
covariates <- dplyr::as_tibble(querySql(connection, sql, snakeCaseToCamelCase = TRUE))

swap <- function(data, column1, column2) {
  temp <- data[column1]
  data[column1] <- data[column2]
  data[column2] <- temp
  return(data)
}
ctCovariates <- swap(covariates, "tCohortId", "cCohortId")
ctCovariates <- swap(ctCovariates, "tCohortName", "cCohortName")
ctCovariates <- swap(ctCovariates, "tPersons", "cPersons")
ctCovariates <- swap(ctCovariates, "tMean", "cMean")
covariates <- covariates %>%
  bind_rows(ctCovariates)
saveRDS(covariates, file.path(workingFolder, "highCorrelationCovars.rds"))
disconnect(connection)

# Filter by TCs of interest ------------------------------------------------------------------
covariates <- readRDS(file.path(workingFolder, "highCorrelationCovars.rds"))
tcos <- readr::read_csv("inst/settings/TcosOfInterest.csv")
covariates <- covariates %>%
  inner_join(distinct(tcos, tCohortId = .data$targetId, cCohortId = .data$comparatorId),
             by = c("tCohortId", "cCohortId"))
saveRDS(covariates, file.path(workingFolder, "highCorrelationCovarsFilterByTc.rds"))

# Get concept IDs of target and comparator drugs -------------------------------------------------
deriveFromCohortJson <- FALSE

if (deriveFromCohortJson) {
  cohorts <- ScyllaCharacterization::getCohortsToCreate() %>%
    filter(.data$cohortType == "target")

  getCodeSetId <- function(criterion) {
    if (is.list(criterion)) {
      criterion$CodesetId
    } else if (is.vector(criterion)) {
      return(criterion["CodesetId"])
    } else {
      return(NULL)
    }
  }

  getCodeSetIds <- function(criterionList) {
    codeSetIds <- lapply(criterionList, getCodeSetId)
    codeSetIds <- do.call(c, codeSetIds)
    if (is.null(codeSetIds)) {
      return(NULL)
    } else {
      return(dplyr::tibble(domain = names(criterionList), codeSetIds = codeSetIds)
             %>% filter(!is.na(codeSetIds)))
    }
  }

  getConceptsInSet <- function(conceptSet) {
    return(sapply(conceptSet$expression$items, function(x) x$concept$CONCEPT_ID))
  }

  getConcepts <- function(cohortId) {
    cohortDefinition <- RJSONIO::fromJSON(system.file("cohorts", paste0(cohortId, ".json"), package = "ScyllaCharacterization"))
    primaryCodesetIds <- lapply(cohortDefinition$PrimaryCriteria$CriteriaList, getCodeSetIds) %>%
      dplyr::bind_rows()
    conceptIds <- c()
    for (conceptSet in cohortDefinition$ConceptSets) {
      if (conceptSet$id %in% primaryCodesetIds$codeSetIds) {
        conceptIds <- c(conceptIds, getConceptsInSet(conceptSet))
      }
    }
    return(tibble(cohortId = rep(cohortId, length(conceptIds)), conceptId = conceptIds))
  }

  cohortConcepts <- lapply(cohorts$cohortId, getConcepts)
  cohortConcepts <- bind_rows(cohortConcepts)
} else {
  # deriveFromCohortJson == FALSE
  pathToCsv <- system.file("settings", "TargetCohortConceptIds.csv", package = "ScyllaEstimation")
  cohortConcepts <- readr::read_csv(pathToCsv)
  splitConceptIds <- function(row) {
    return(dplyr::tibble(cohortId = row$cohortId,
                         conceptId= as.numeric(strsplit(row$conceptIds, ";")[[1]])))
  }
  cohortConcepts <- lapply(split(cohortConcepts, 1:nrow(cohortConcepts)), splitConceptIds)
  cohortConcepts <- bind_rows(cohortConcepts)
}

saveRDS(cohortConcepts, file.path(workingFolder, "cohortConcepts.rds"))

# Expand cohort concept IDs to ancestors and descendants ------------------------------------------------------
cohortConcepts <- readRDS(file.path(workingFolder, "cohortConcepts.rds"))
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = Sys.getenv("PDW_SERVER"),
                                                                user = NULL,
                                                                password = NULL,
                                                                port = Sys.getenv("PDW_PORT"))
# cdmDatabaseSchema <- "CDM_Premier_COVID_V1260.dbo"
# cdmDatabaseSchema <- "Vocabulary_20200320.dbo"
cdmDatabaseSchema <- "CDM_OPTUM_EHR_COVID_v1351.dbo"

connection <- connect(connectionDetails)
sql <- "SELECT descendant_concept_id AS concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (@concept_ids)

UNION

SELECT ancestor_concept_id AS concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE descendant_concept_id IN (@concept_ids);"

getAncestorsAndDescendants <- function(cohortId) {
  conceptIds <- cohortConcepts$conceptId[cohortConcepts$cohortId == cohortId]
  ancestorsAndDescendants <- renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     concept_ids = conceptIds,
                                                     snakeCaseToCamelCase = TRUE) %>%
    as_tibble()
  ancestorsAndDescendants$cohortId <- rep(cohortId, nrow(ancestorsAndDescendants))
  return(ancestorsAndDescendants)
}
cohortConceptsWithAncestorsAndDescendants <- lapply(unique(cohortConcepts$cohortId), getAncestorsAndDescendants)
cohortConceptsWithAncestorsAndDescendants <- bind_rows(cohortConceptsWithAncestorsAndDescendants)
saveRDS(cohortConceptsWithAncestorsAndDescendants, file.path(workingFolder, "cohortConceptsWithAncestorsAndDescendants.rds"))
disconnect(connection)

# Remove target and comparator concepts from high-correlation covariate lists -----------------------------
cohortConceptsWithAncestorsAndDescendants <- readRDS(file.path(workingFolder, "cohortConceptsWithAncestorsAndDescendants.rds"))
highCorrCovariates <- readRDS(file.path(workingFolder, "highCorrelationCovarsFilterByTc.rds"))
# subset <- split(highCorrCovariates, paste(highCorrCovariates$tCohortId, highCorrCovariates$cCohortId))[[1]]
# subset <- highCorrCovariates[highCorrCovariates$tCohortId == 1007020011 & highCorrCovariates$cCohortId == 1009020011, ]
removeDrugConcepts <- function(subset) {
  targetCohortId <- round(subset$tCohortId[1] / 1e6)
  comparatorCohortId <- round(subset$cCohortId[1] / 1e6)
  targetConceptIds <- cohortConceptsWithAncestorsAndDescendants %>%
    filter(.data$cohortId == targetCohortId) %>%
    pull(conceptId)
  comparatorConceptIds <- cohortConceptsWithAncestorsAndDescendants %>%
    filter(.data$cohortId == comparatorCohortId) %>%
    pull(conceptId)
  subset <- subset %>%
    mutate(conceptId = round(.data$covariateId / 10000)) %>%
    filter(!.data$conceptId %in% c(targetConceptIds, comparatorConceptIds)) %>%
    select(-.data$conceptId)
  return(subset)
}
covariates <- lapply(split(highCorrCovariates, paste(highCorrCovariates$tCohortId, highCorrCovariates$cCohortId)), removeDrugConcepts)
covariates <- bind_rows(covariates)
readr::write_csv(covariates, file.path(workingFolder, "highCorrelationCovariates.csv"))

# Sanity check: replicate clavulanate as high-correlation covariate when comparing azithromycin to ammoxycillin:
covariates %>%
  filter(round(.data$tCohortId / 1e6) == 1007 & round(.data$cCohortId / 1e6) == 1009) %>%
  filter(grepl("Clavulanate", .data$covariateName))
# Note: is empty when not using JSON to derive cohort concepts, because clavulenate was already excluded

x <- covariates %>%
  filter(round(.data$tCohortId / 1e6) == 1007 & round(.data$cCohortId / 1e6) == 1009 & .data$databaseId == "CDM_Premier_COVID_v1260")
View(x)



x <- cohortConceptsWithAncestorsAndDescendants[cohortConceptsWithAncestorsAndDescendants$cohortId == 1008, ]
x[x$conceptId == 43534855,]

sql <- "SELECT * FROM CDM_Premier_COVID_V1260.dbo.concept_ancestor WHERE ancestor_concept_id = 43534855 AND descendant_concept_id = 1797513;"
renderTranslateQuerySql(connection = connection,
                        sql = sql,
                        cdm_database_schema = cdmDatabaseSchema,
                        snakeCaseToCamelCase = TRUE)


# Find overlap in cohorts between Scylla Characterization and Estimation ----------------------------
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("scyllaServer"),
                                                            keyring::key_get("scyllaDatabase"), sep = "/"),
                                             user = keyring::key_get("scyllaUser"),
                                             password = keyring::key_get("scyllaPassword"))
connection <- connect(connectionDetails)
cohortsChar <- querySql(connection = connection,
                        sql = "SELECT cohort.*, cohort_entries, cohort_subjects, database_id FROM scylla.cohort INNER JOIN scylla.cohort_count ON cohort.cohort_id = cohort_count.cohort_id;",
                        snakeCaseToCamelCase = TRUE)
disconnect(connection)
cohortsChar <- cohortsChar %>%
  filter(.data$databaseId == "OptumEhr1351")
cohortsEst <- read.csv(file.path(workingFolder, "OptumEhr", "cohort_count.csv"))

cohortsChar %>%
  filter(.data$targetId == 1046)

sql <- "
SELECT cohort.*,
  covariate_value.covariate_id,
  covariate_value.mean,
  covariate_value.sd,
  covariate_value.database_id,
  covariate.covariate_name
FROM scylla.cohort
INNER JOIN scylla.covariate_value
  ON cohort.cohort_id = covariate_value.cohort_id
INNER JOIN scylla.covariate
  ON covariate_value.covariate_id = covariate.covariate_id
WHERE target_id IN (1045, 1046)
  AND database_id = 'OptumEhr1351';"

covs <- querySql(connection = connection,
                 sql = sql,
                 snakeCaseToCamelCase = TRUE) %>%
  as_tibble()

covs %>%
  filter(grepl("cardiovascular disease", .data$covariateName)) %>%
  filter(.data$subgroupId == 0) %>%
  arrange(.data$covariateId, .data$targetId)


library(CohortMethod)
cmData <- loadCohortMethodData("s:/ScyllaEstimation/OptumEhr/cmOutput/CmData_l1_t1045000011_c1046000011.zip")
cmData <- loadCohortMethodData("s:/ScyllaEstimation/OptumEhr/cmOutput/CmData_l1_t1071000011_c1072000011.zip")
cmData <- loadCohortMethodData("s:/ScyllaEstimation/OptumEhr/cmOutput/CmData_l1_t1070000011_c1074000011.zip")

studyPop <- createStudyPopulation(cohortMethodData = cmData,
                                  firstExposureOnly = TRUE,
                                  washoutPeriod = 365,
                                  removeDuplicateSubjects = "keep first",
                                  removeSubjectsWithPriorOutcome = TRUE,
                                  riskWindowStart = 1,
                                  startAnchor = "cohort start",
                                  riskWindowEnd = 9999,
                                  endAnchor = "cohort end",
                                  censorAtNewRiskWindow = TRUE)
getAttritionTable(studyPop)
ps <- createPs(cohortMethodData = cmData,
               stopOnError = FALSE,
               population = studyPop)
plotPs(ps)
sum(studyPop$treatment == 1)
sum(studyPop$treatment == 0)
summary(cmData)
getAttritionTable(cmData)
cohortsEst %>%
  filter(.data$cohort_id == 1045000011)
cohortsChar %>%
  filter(.data$targetId == 1046)


# Find out why cohort counts are so low
connection <- connect(connectionDetails)
allEntries <- querySql(connection, "SELECT * FROM scratch.dbo.mschuemi_scylla_estimation_optum_ehr WHERE cohort_definition_id = 1045000011;")
querySql(connection, "SELECT COUNT(*) FROM scratch.dbo.mschuemi_scylla_estimation_optum_ehr WHERE cohort_definition_id = 1045000011;")
cmData <- getDbCohortMethodData(connectionDetails = connectionDetails,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                targetId = 1045000011,
                                comparatorId = 1046000011,
                                outcomeIds = 152,
                                studyStartDate = "20200101",
                                firstExposureOnly = TRUE,
                                removeDuplicateSubjects = FALSE,
                                restrictToCommonPeriod = TRUE,
                                exposureDatabaseSchema = cohortDatabaseSchema,
                                exposureTable = cohortTable,
                                outcomeDatabaseSchema = cohortDatabaseSchema,
                                outcomeTable = cohortTable,
                                covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE))
getAttritionTable(cmData)

# Find high-correlation covariates in 'large' cohorts -------------------------------------------------------------------
# Requires CohortMethod to be executed using stopOnError = FALSE
library(dplyr)
folder <- "s:/ScyllaEstimation/OptumEhr/cmOutput"
minCohortSize <- 100
omr <- readRDS(file.path(folder, "outcomeModelReference.rds"))
omrFiltered <- omr[!omr$sharedPsFile == "", ]
omrFiltered <- omrFiltered[!duplicated(paste(omrFiltered$targetId, omrFiltered$comparatorId)), ]
covs <- tibble::tibble()
for (i in 1:nrow(omrFiltered)) {
  ps <- readRDS(file.path(folder, omrFiltered$sharedPsFile[i]))
  if (sum(ps$treatment == 1) >= minCohortSize && sum(ps$treatment == 0) >= minCohortSize) {
    if (!is.null(attr(ps, "metaData")$psHighCorrelation)) {
      x <- attr(ps, "metaData")$psHighCorrelation
      x$targetId <- omrFiltered$targetId[i]
      x$targetSize <- sum(ps$treatment == 1)
      x$comparatorId <- omrFiltered$comparatorId[i]
      x$comparatorSize <- sum(ps$treatment == 0)
      cmData <- CohortMethod::loadCohortMethodData(file.path(folder, omrFiltered$cohortMethodDataFile[i]))
      covariateIds <- x$covariateId
      cohortCounts <- cmData$cohorts %>%
        group_by(.data$treatment) %>%
        summarise(cohortCount = n()) %>%
        ungroup()
      covProps <- cmData$covariates %>%
        filter(.data$covariateId %in% covariateIds) %>%
        inner_join(cmData$cohorts, by = "rowId") %>%
        group_by(.data$treatment, .data$covariateId) %>%
        summarise(featureCount = n()) %>%
        ungroup() %>%
        inner_join(cohortCounts, by = "treatment") %>%
        mutate(proportion = featureCount / as.numeric(cohortCount)) %>%
        collect()
      x <- x %>%
        left_join(select(filter(covProps, .data$treatment == 1),
                         .data$covariateId,
                         targetProportion = .data$proportion),
                  by = "covariateId") %>%
        left_join(select(filter(covProps, .data$treatment == 0),
                         .data$covariateId,
                         comparatorProportion = .data$proportion),
                  by = "covariateId")
      covs <- dplyr::bind_rows(covs, x)
#
#
#       ps$propensityScore <- NULL
#       ps$preferenceScore <- NULL
#       ps2 <- CohortMethod::createPs(cohortMethodData = cmData,
#                                     population = ps,
#                                     errorOnHighCorrelation = TRUE)
#       CohortMethod::plotPs(ps2)
#
#       highCorCov <- cmData$cohorts %>%
#         left_join (cmData$covariates %>%
#                      filter(.data$covariateId == 8532001),
#                    by = "rowId") %>%
#         mutate(value = case_when(is.na(.data$covariateValue) ~ 0, TRUE ~ .data$covariateValue)) %>%
#         collect()
#       cor(highCorCov$treatment, highCorCov$value)
    }
  }
}
covs <- covs %>%
  mutate(covariateShortName = case_when(grepl("index month", .data$covariateName) ~ .data$covariateName,
                                        TRUE ~gsub(".*: ", "", .data$covariateName)))
pathToCsv <- file.path(outputFolder, "cohort_count.csv")
cohorts <- readr::read_csv(pathToCsv, col_types = readr::cols())
cohorts <- cohorts %>%
  mutate(cohortShortName = gsub(" with.*", "", .data$name))

forReview <- covs %>%
  select(.data$targetId,
         .data$comparatorId,
         .data$targetSize,
         .data$comparatorSize,
         .data$covariateShortName,
         .data$targetProportion,
         .data$comparatorProportion) %>%
  inner_join(select(cohorts, targetId = .data$cohort_id,
                    targetName = .data$cohortShortName),
             by = "targetId") %>%
  inner_join(select(cohorts, comparatorId = .data$cohort_id,
                    comparatorName = .data$cohortShortName),
             by = "comparatorId")

readr::write_csv(forReview, "s:/ScyllaEstimation/OptumEhr/HighCorrelationCovs.csv")
