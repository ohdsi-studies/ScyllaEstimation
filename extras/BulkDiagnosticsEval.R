# Some code for evaluate study diagnostics across all performed analyses


library(dplyr)
outputFolder <- "s:/ScyllaEstimation/OptumEhr"

addCohortNamesAndAnalysisDescriptions <- function(table) {
  exposures <- readr::read_csv(file.path(outputFolder, "export", "exposure_of_interest.csv"))
  table <- inner_join(table,
                      tibble(targetId = exposures$exposure_id,
                             targetName = exposures$exposure_name),
                      by = "targetId")
  table <- inner_join(table,
                      tibble(comparatorId = exposures$exposure_id,
                             comparatorName = exposures$exposure_name),
                      by = "comparatorId")
  cmAnalysisListFile <- system.file("settings",
                                    "cmAnalysisList.json",
                                    package = "ScyllaEstimation")
  cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)
  cmAnalysisToRow <- function(cmAnalysis) {
    row <- tibble(analysisId = cmAnalysis$analysisId,
                  description = cmAnalysis$description)
    return(row)
  }
  cohortMethodAnalysis <- lapply(cmAnalysisList, cmAnalysisToRow)
  cohortMethodAnalysis <- do.call("rbind", cohortMethodAnalysis)
  cohortMethodAnalysis <- unique(cohortMethodAnalysis)
  table <- inner_join(table, cohortMethodAnalysis, by = "analysisId")
  return(table)
}

# Balance ------------------------------------------------------------------------
balanceFolder <- file.path(outputFolder, "balance")
files <- list.files(balanceFolder, pattern = "bal_.*.rds", full.names = TRUE)

getMaxAbsSd <- function(file) {
  ids <- gsub("^.*bal_t", "", file)
  targetId <- as.numeric(gsub("_c.*", "", ids))
  ids <- gsub("^.*_c", "", ids)
  comparatorId <- as.numeric(gsub("_[aso].*$", "", ids))
  if (grepl("_s", ids)) {
    subgroupId <- as.numeric(gsub("^.*_s", "", gsub("_a[0-9]*.rds", "", ids)))
  } else {
    subgroupId <- NA
  }
  if (grepl("_o", ids)) {
    outcomeId <- as.numeric(gsub("^.*_o", "", gsub("_a[0-9]*.rds", "", ids)))
  } else {
    outcomeId <- NA
  }
  ids <- gsub("^.*_a", "", ids)
  analysisId <- as.numeric(gsub(".rds", "", ids))
  balance <- readRDS(file)
  nCovariates <- nrow(balance)
  maxSd <- max(abs(balance$afterMatchingStdDiff), na.rm = TRUE)
  if (maxSd == 0) {
    maxSd <- NA
  }
  return(tibble(targetId = targetId,
                comparatorId = comparatorId,
                analysisId = analysisId,
                nCovariates = nCovariates,
                maxSd = maxSd))
}
stats <- plyr::llply(files, getMaxAbsSd, .progress = "text")
stats <- bind_rows(stats)
stats <- addCohortNamesAndAnalysisDescriptions(stats)
readr::write_csv(stats, file.path(outputFolder, sprintf("BalanceOverview_%s.csv", databaseId)))
x <- stats[stats$maxSd < 0.1, ]
x$targetName

# Negative controls --------------------------------------------------
analysesSum <- readr::read_csv(file.path(outputFolder, "analysisSummary.csv"), col_types = readr::cols(), guess_max = 1e6)
negativeControls <- ScyllaEstimation:::getNegativeControlOutcomes()
ncOutcomeIds <- negativeControls$outcomeId
analysesSum <- analysesSum %>%
  filter(.data$outcomeId %in% ncOutcomeIds & !is.na(.data$seLogRr))

analysesSum <- analysesSum %>%
  group_by(.data$analysisId, .data$targetId, .data$comparatorId, .data$targetName, .data$comparatorName, .data$description) %>%
  summarise(nNegativeControls = n(),
            targetSubjects = max(.data$target),
            comparatorSubjects = max(.data$comparator),
            minSe = min(.data$seLogRr),
            medianSe = median(.data$seLogRr)) %>%
  ungroup()

readr::write_csv(analysesSum, file.path(outputFolder, sprintf("NegativeControlsOverview_%s.csv", databaseId)))


x <- analysesSum[analysesSum$nNegativeControls == max(analysesSum$nNegativeControls), ]
sprintf("targetId = %d; comparatorId = %d; analysisId = %d", x$targetId, x$comparatorId, x$analysisId)


# Imbalanced covariates  ------------------------------------------------------------------------
balanceFolder <- file.path(outputFolder, "balance")
files <- list.files(balanceFolder, pattern = "bal_.*.rds", full.names = TRUE)

getImbalancedCovariates <- function(file, bounds = c(0.1, 0.2), minCohortSize = 100) {
  ids <- gsub("^.*bal_t", "", file)
  targetId <- as.numeric(gsub("_c.*", "", ids))
  ids <- gsub("^.*_c", "", ids)
  comparatorId <- as.numeric(gsub("_[aso].*$", "", ids))
  if (grepl("_s", ids)) {
    subgroupId <- as.numeric(gsub("^.*_s", "", gsub("_a[0-9]*.rds", "", ids)))
  } else {
    subgroupId <- NA
  }
  if (grepl("_o", ids)) {
    outcomeId <- as.numeric(gsub("^.*_o", "", gsub("_a[0-9]*.rds", "", ids)))
  } else {
    outcomeId <- NA
  }
  ids <- gsub("^.*_a", "", ids)
  analysisId <- as.numeric(gsub(".rds", "", ids))
  balance <- readRDS(file)
  inferredTargetAfterSize <- mean(balance$afterMatchingSumTarget/balance$afterMatchingMeanTarget,
                                  na.rm = TRUE)
  inferredComparatorAfterSize <- mean(balance$afterMatchingSumComparator/balance$afterMatchingMeanComparator,
                                      na.rm = TRUE)
  if (is.nan(inferredTargetAfterSize ) ||
      inferredTargetAfterSize > minCohortSize ||
      is.nan(inferredComparatorAfterSize ) ||
      inferredComparatorAfterSize < minCohortSize) {
    return(NULL)
  }
  balance <- balance %>%
    filter(abs(.data$afterMatchingStdDiff) > bounds[1] & abs(.data$afterMatchingStdDiff) < bounds[2])
  balance$targetId <- rep(targetId, nrow(balance))
  balance$comparatorId <- rep(comparatorId, nrow(balance))
  balance$analysisId <- rep(analysisId, nrow(balance))
  return(balance)
}
stats <- plyr::llply(files, getImbalancedCovariates, .progress = "text")
stats <- bind_rows(stats)
stats <- addCohortNamesAndAnalysisDescriptions(stats)
stats$targetShortName <- gsub(" with.*", "", stats$targetName)
stats$comparatorShortName <- gsub(" with.*", "", stats$comparatorName)
readr::write_csv(stats, file.path(outputFolder, sprintf("ImbalancedCovariates_%s.csv", databaseId)))
sum(abs(stats$beforeMatchingStdDiff  > 0.2))

# Balance from exported zip file ------------------------------------------------------------
library(dplyr)
zipFile <-  "s:/ScyllaEstimation/AllDbs/Results_SIDIAP.zip"
unzipFolder <- tempfile()
dir.create(unzipFolder)
unzip(zipFile, exdir = unzipFolder)
list.files(unzipFolder)
balance <- readr::read_csv(file.path(unzipFolder, "covariate_balance.csv"))
colnames(balance) <- SqlRender::snakeCaseToCamelCase(colnames(balance))
balance <- balance[!is.na(balance$stdDiffAfter) & is.na(balance$outcomeId), ]
balance <- aggregate(abs(stdDiffAfter) ~ targetId + comparatorId + analysisId, balance, max)
balance[balance$`abs(stdDiffAfter)` < 0.1, ]

exposures <- readr::read_csv(file.path(unzipFolder, "exposure_of_interest.csv"))
balance <- inner_join(balance,
                    tibble(targetId = exposures$exposure_id,
                           targetName = exposures$exposure_name),
                    by = "targetId")
balance <- inner_join(balance,
                    tibble(comparatorId = exposures$exposure_id,
                           comparatorName = exposures$exposure_name),
                    by = "comparatorId")

readr::write_csv(balance, "s:/ScyllaEstimation/AllDbs/BalanceOverview_SIDIAP.csv")
unlink(unzipFolder, recursive = TRUE)

# Balance from results database -----------------------------------------------------------
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("scyllaServer"),
                                                            keyring::key_get("scyllaDatabase"),
                                                            sep = "/"),
                                             user = keyring::key_get("scyllaUser"),
                                             password = keyring::key_get("scyllaPassword"))
schema <- "scylla_estimation"
connection <- connect(connectionDetails)
sql <- "SELECT max_sd,
  database_id,
  target_id,
  target.exposure_name AS target_name,
  comparator_id,
  comparator.exposure_name AS comparator_name,
  tmp.analysis_id,
  description AS analysis_description
FROM (
  SELECT MAX(ABS(std_diff_after)) AS max_sd,
    database_id,
    target_id,
    comparator_id,
    analysis_id
  FROM @schema.covariate_balance
  WHERE outcome_id = -1
    AND std_diff_after IS NOT NULL
    AND std_diff_after != 0
  GROUP BY database_id,
    target_id,
    comparator_id,
    analysis_id) tmp
INNER JOIN @schema.exposure_of_interest target
  ON target_id = target.exposure_id
INNER JOIN @schema.exposure_of_interest comparator
  ON comparator_id = comparator.exposure_id
INNER JOIN @schema.cohort_method_analysis
  ON tmp.analysis_id = cohort_method_analysis.analysis_id;"

results <- renderTranslateQuerySql(connection, sql, schema = schema, snakeCaseToCamelCase = TRUE)
disconnect(connection)
results <- results[order(results$maxSd), ]
readr::write_csv(results, "s:/ScyllaEstimation/AllDbs/BalanceOverview_AllDbs.csv")




sql <- "SELECT *
FROM @schema.covariate_balance
WHERE outcome_id = -1
 AND database_id = 'HealthVerity'
 AND target_id = 1001020021
 AND comparator_id = 1002020021
 AND  analysis_id = 419;"

x <- renderTranslateQuerySql(connection, sql, schema = schema, snakeCaseToCamelCase = TRUE)
