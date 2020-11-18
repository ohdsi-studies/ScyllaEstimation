# Some code for evaluate study diagnostics across all performed analyses


library(dplyr)
outputFolder <- "s:/ScyllaEstimation/OptumEhr"

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
exposures <- readr::read_csv(file.path(outputFolder, "export", "exposure_of_interest.csv"))
stats <- inner_join(stats,
                    tibble(targetId = exposures$exposure_id,
                           targetName = exposures$exposure_name),
                    by = "targetId")
stats <- inner_join(stats,
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
stats <- inner_join(stats, cohortMethodAnalysis, by = "analysisId")
readr::write_csv(stats, file.path(outputFolder, "BalanceOverview.csv"))
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

readr::write_csv(analysesSum, file.path(outputFolder, "NegativeControlsOverview.csv"))


x <- analysesSum[analysesSum$nNegativeControls == max(analysesSum$nNegativeControls), ]
sprintf("targetId = %d; comparatorId = %d; analysisId = %d", x$targetId, x$comparatorId, x$analysisId)

