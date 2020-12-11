# Analysing some of the results in the results database
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("scyllaServer"),
                                                            keyring::key_get("scyllaDatabase"),
                                                            sep = "/"),
                                             user = keyring::key_get("scyllaUser"),
                                             password = keyring::key_get("scyllaPassword"))
schema <- "scylla_estimation"
resultsFolder <- "S:/ScyllaEstimation/ResultsAnalyses"
if (!file.exists(resultsFolder))
  dir.create(resultsFolder)

# Find analyses meeting our balance criterium -----------------------
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

results <- results[results$maxSd < 0.1, ]
readr::write_csv(results, file.path(resultsFolder, "AnalysesHavingBalance.csv"))

# Fetch balance statistics for balanced analyses --------------------------------------
library(dplyr)
balanced <- readr::read_csv(file.path(resultsFolder, "AnalysesHavingBalance.csv"), col_types = readr::cols())
balanced <- balanced %>%
  select(databaseId, targetId, comparatorId, analysisId)

connection <- connect(connectionDetails)
DatabaseConnector::insertTable(connection = connection,
                               tableName = "#temp_table",
                               data = as.data.frame(balanced),
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = TRUE,
                               camelCaseToSnakeCase = TRUE)
sql <- "SELECT covariate_balance.*
FROM @schema.covariate_balance
INNER JOIN #temp_table temp_table
  ON covariate_balance.database_id = temp_table.database_id
    AND covariate_balance.target_id = temp_table.target_id
    AND covariate_balance.comparator_id = temp_table.comparator_id
    AND covariate_balance.analysis_id = temp_table.analysis_id
WHERE covariate_balance.outcome_id = -1;"
results <- renderTranslateQuerySql(connection, sql, schema = schema, snakeCaseToCamelCase = TRUE)
disconnect(connection)
saveRDS(results, file.path(resultsFolder, "BalancedBalance.rds"))

plotBalance <- function(balance) {
  beforeLabel = "Before PS adjustment"
  afterLabel = "After PS adjustment"
  balance$absBeforeMatchingStdDiff <- abs(balance$stdDiffBefore)
  balance$absAfterMatchingStdDiff <- abs(balance$stdDiffAfter)
  limits <- c(min(c(balance$absBeforeMatchingStdDiff, balance$absAfterMatchingStdDiff),
                  na.rm = TRUE),
              max(c(balance$absBeforeMatchingStdDiff, balance$absAfterMatchingStdDiff),
                  na.rm = TRUE))
  theme <- ggplot2::element_text(colour = "#000000", size = 12)
  plot <- ggplot2::ggplot(balance,
                          ggplot2::aes(x = absBeforeMatchingStdDiff, y = absAfterMatchingStdDiff)) +
    ggplot2::geom_point(color = rgb(0, 0, 0.8, alpha = 0.3), shape = 16, size = 2) +
    ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +
    ggplot2::scale_x_continuous(beforeLabel, limits = limits) +
    ggplot2::scale_y_continuous(afterLabel, limits = limits) +
    ggplot2::theme(text = theme)
  fileName <- file.path(resultsFolder, sprintf("Bal_t%d_c%d_a%d_%s.png", balance$targetId[1], balance$comparatorId[1], balance$analysisId[1], balance$databaseId[1]))
  ggplot2::ggsave(fileName, plot, width = 4, height = 4, dpi = 400)
}
plyr::l_ply(split(results, paste(results$databaseId, results$targetId, results$comparatorId, results$analysisId)), plotBalance, .progress = "text")

# Fetch all estimates for balanced analyses ---------------------------
library(dplyr)
balanced <- readr::read_csv(file.path(resultsFolder, "AnalysesHavingBalance.csv"), col_types = readr::cols())
balanced <- balanced %>%
  select(databaseId, targetId, comparatorId, analysisId)

connection <- connect(connectionDetails)
DatabaseConnector::insertTable(connection = connection,
                               tableName = "#temp_table",
                               data = as.data.frame(balanced),
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = TRUE,
                               camelCaseToSnakeCase = TRUE)
sql <- "SELECT cohort_method_result.*,
  type,
  target.exposure_name AS target_name,
  comparator.exposure_name AS comparator_name,
  outcome_name,
  description AS analysis_description
FROM @schema.cohort_method_result
INNER JOIN #temp_table temp_table
  ON cohort_method_result.database_id = temp_table.database_id
    AND cohort_method_result.target_id = temp_table.target_id
    AND cohort_method_result.comparator_id = temp_table.comparator_id
    AND cohort_method_result.analysis_id = temp_table.analysis_id
INNER JOIN @schema.exposure_of_interest target
  ON cohort_method_result.target_id = target.exposure_id
INNER JOIN @schema.exposure_of_interest comparator
  ON cohort_method_result.comparator_id = comparator.exposure_id
INNER JOIN @schema.cohort_method_analysis
  ON cohort_method_result.analysis_id = cohort_method_analysis.analysis_id
INNER JOIN (
  SELECT outcome_id,
    outcome_name,
    CAST('hoi' AS VARCHAR) AS type
  FROM @schema.outcome_of_interest

  UNION

  SELECT outcome_id,
    outcome_name,
    CAST('negative control' AS VARCHAR) AS type
  FROM @schema.negative_control_outcome) tmp
  ON cohort_method_result.outcome_id = tmp.outcome_id;"
results <- renderTranslateQuerySql(connection, sql, schema = schema, snakeCaseToCamelCase = TRUE)
disconnect(connection)
saveRDS(results, file.path(resultsFolder, "BalancedEstimates.rds"))

plotNcs <- function(subset) {
  ncs <- subset[subset$type == "negative control", ]
  ncs <- ncs[!is.na(ncs$seLogRr), ]
  if (nrow(ncs) == 0) {
    writeLines(sprintf("No negative control estimates for target %d, comparator %d, analysis %d, database, %s", ncs$targetId[1], ncs$comparatorId[1], ncs$analysisId[1], ncs$databaseId[1]))
    return(NULL)
  }
  fileName <- file.path(resultsFolder, sprintf("Ncs_t%d_c%d_a%d_%s.png", ncs$targetId[1], ncs$comparatorId[1], ncs$analysisId[1], ncs$databaseId[1]))
  EmpiricalCalibration::plotCalibrationEffect(logRrNegatives = ncs$logRr,
                                              seLogRrNegatives = ncs$seLogRr,
                                              showCis = TRUE,
                                              fileName = fileName)
}
plyr::l_ply(split(results, paste(results$databaseId, results$targetId, results$comparatorId, results$analysisId)), plotNcs, .progress = "text")

hoiEstimates <- results[results$type == "hoi", ]
hoiEstimates <- hoiEstimates[!is.na(hoiEstimates$seLogRr) & !is.na(hoiEstimates$ci95Lb) & !is.na(hoiEstimates$ci95Ub), ]
hoiEstimates <- hoiEstimates %>%
  select(databaseId, analysisId, analysisDescription, targetId, targetName, comparatorId, comparatorName, outcomeId, outcomeName, rr, ci95Lb, ci95Ub, p, targetSubjects, comparatorSubjects, targetDays, comparatorDays, targetOutcomes, comparatorOutcomes) %>%
  arrange(targetName, comparatorName, outcomeName)
readr::write_csv(hoiEstimates, file.path(resultsFolder, "EstimatesCovarBalanceNoCalibrationNonReproduced.csv"))
nrow(hoiEstimates)
0.05 * nrow(hoiEstimates)
sum(hoiEstimates$p < 0.05)

tcoDbs <- hoiEstimates %>%
  distinct(databaseId, targetId, comparatorId, outcomeName)
nrow(tcoDbs)
0.05 * nrow(tcoDbs)

sigTcoDbs <- hoiEstimates %>%
  filter(p < 0.05) %>%
  distinct(databaseId, targetId, comparatorId, outcomeName)
nrow(sigTcoDbs)
