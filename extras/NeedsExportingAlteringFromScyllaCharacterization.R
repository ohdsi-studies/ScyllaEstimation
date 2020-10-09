loadFile <- function(fileInput) {
  fileName <- system.file("settings", fileInput[1], package = fileInput[2])
  df <-  read.csv(fileName)
  return(df)
}

getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortCounts.sql",
                                           packageName = "ScyllaCharacterization",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = cohortIds)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Counting cohorts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
}

getAllStudyCohorts <- function() {
  cohortsToCreate <- getCohortsToCreate()
  targetSubgroupXref <- getTargetSubgroupXref()
  colNames <- c("name", "cohortId")
  cohortsToCreate <- cohortsToCreate[, match(colNames, names(cohortsToCreate))]
  targetSubgroupXref <- targetSubgroupXref[, match(colNames, names(targetSubgroupXref))]
  allCohorts <- rbind(cohortsToCreate, targetSubgroupXref)
  return(allCohorts)
}

writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    params$data = data
    params$fileName = fileName
    do.call(saveIncremental, params)
  } else {
    readr::write_csv(data, fileName)
  }
}

getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 || length(key[[1]]) == 0 || !all(names(key) %in% names(recordKeeping))) {
    return(c())
  } else {
    key <- unique(tibble::as_tibble(key))
    recordKeeping$idxCol <- 1:nrow(recordKeeping)
    idx <- merge(recordKeeping, key)$idx
    return(idx)
  }
}

saveIncremental <- function(data, fileName, ...) {
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(fileName)) {
    previousData <- readr::read_csv(fileName, col_types = readr::cols())
    idx <- getKeyIndex(list(...), previousData)
    if (length(idx) > 0) {
      previousData <- previousData[-idx, ]
    }
    data <- dplyr::bind_rows(previousData, data)
  }
  readr::write_csv(data, fileName)
}



