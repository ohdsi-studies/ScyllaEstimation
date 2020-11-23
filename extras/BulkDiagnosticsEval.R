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
