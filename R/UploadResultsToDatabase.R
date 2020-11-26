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

#' Upload results to the database server.
#'
#' @description
#' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' bulk upload (recommended).
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables will be created.
#' @param zipFileName    The name of the zip file.
#' @param createTables   Create the tables on the server? If TRUE and the tables already exist they will be overwritten!
#' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#'                       will be overwritten if they already exist on the database. Only use this if these specifications
#'                       have changed since the last upload.
#' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#'                       that site will be dropped. This assumes the input zip file contains the full data for that
#'                       data site.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficent space if the default system temp space is too limited.
#'
#' @export
uploadResultsToDatabase <- function(connectionDetails = NULL,
                                    schema,
                                    zipFileName,
                                    createTables = FALSE,
                                    forceOverWriteOfSpecifications = FALSE,
                                    purgeSiteDataBeforeUploading = TRUE,
                                    tempFolder = tempdir()) {
  if (createTables) {
    purgeSiteDataBeforeUploading <- FALSE
  }
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)

  ParallelLogger::logInfo("Unzipping ", zipFileName)
  zip::unzip(zipFileName, exdir = unzipFolder)

  if (purgeSiteDataBeforeUploading) {
    database <- readr::read_csv(file = file.path(unzipFolder, "database.csv"), col_types = readr::cols())
    colnames(database) <- SqlRender::snakeCaseToCamelCase(colnames(database))
    databaseId <- database$databaseId
  }

  uploadTable <- function(file) {
    tableName <- gsub(".csv", "", file)
    ParallelLogger::logInfo("Uploading table ", tableName)

    columns <- colnames(readr::read_csv(file.path(unzipFolder, file), n_max = 0, col_types = readr::cols()))
    if (purgeSiteDataBeforeUploading && "database_id" %in% columns) {
      deleteAllRecordsForDatabaseId(connection = connection,
                                    schema = schema,
                                    tableName = tableName,
                                    databaseId = databaseId)
    }

    uploadChunk <- function(chunk, pos) {
      ParallelLogger::logInfo("- Uploading rows ", pos, " through ", pos + nrow(chunk) - 1)
      insertDataIntoDb(connection = connection,
                       connectionDetails = connectionDetails,
                       schema = schema,
                       tableName = tableName,
                       createTable = (createTables & pos == 1),
                       data = chunk)
    }

    readr::read_csv_chunked(file = file.path(unzipFolder, file),
                            callback = uploadChunk,
                            chunk_size = 1e7,
                            col_types = readr::cols(),
                            guess_max = 1e6,
                            progress = FALSE)

    # chunk <- readr::read_csv(file = file.path(unzipFolder, file),
    #                          col_types = readr::cols(),
    #                          guess_max = 1e6)
    # pos <- 1

  }
  files <- list.files(unzipFolder, "*.csv")
  invisible(lapply(files, uploadTable))
  delta <- Sys.time() - start
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}

deleteFromServer <- function(connection, schema, tableName, keyValues) {
  createSqlStatement <- function(i) {
    sql <- paste0("DELETE FROM ",
                  schema,
                  ".",
                  tableName,
                  "\nWHERE ",
                  paste(paste0(colnames(keyValues), " = '", keyValues[i, ], "'"), collapse = " AND "),
                  ";")
    return(sql)
  }
  batchSize <- 1000
  for (start in seq(1, nrow(keyValues), by = batchSize)) {
    end <- min(start + batchSize - 1, nrow(keyValues))
    sql <- sapply(start:end, createSqlStatement)
    sql <- paste(sql, collapse = "\n")
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE, runAsBatch = TRUE)
  }
}

deleteAllRecordsForDatabaseId <- function(connection,
                                          schema,
                                          tableName,
                                          databaseId) {
  sql <- "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
  sql <- SqlRender::render(sql = sql,
                           schema = schema,
                           table_name = tableName,
                           database_id = databaseId)
  databaseIdCount <- DatabaseConnector::querySql(connection, sql)[, 1]
  if (databaseIdCount != 0) {
    ParallelLogger::logInfo(sprintf("- Found %s rows in  database with database ID '%s'. Deleting all before inserting.",
                                    databaseIdCount,
                                    databaseId))
    sql <- "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
    sql <- SqlRender::render(sql = sql,
                             schema = schema,
                             table_name = tableName,
                             database_id = databaseId)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

insertDataIntoDb <- function(connection,
                             connectionDetails,
                             schema,
                             tableName,
                             createTable,
                             data) {
  if (nrow(data) < 1e4 || is.null(Sys.getenv("POSTGRES_PATH"))) {
    ParallelLogger::logInfo("- Inserting ", nrow(data), " rows into database")
    DatabaseConnector::insertTable(connection = connection,
                                   tableName = paste(schema, tableName, sep = "."),
                                   data = as.data.frame(data),
                                   dropTableIfExists = createTable,
                                   createTable = createTable,
                                   tempTable = FALSE,
                                   progressBar = TRUE)
  } else {
    ParallelLogger::logInfo("- Inserting ", nrow(data), " rows into database using bulk import")
    if (createTable) {
      DatabaseConnector::insertTable(connection = connection,
                                     tableName = paste(schema, tableName, sep = "."),
                                     data = as.data.frame(data)[FALSE, ],
                                     dropTableIfExists = TRUE,
                                     createTable = TRUE,
                                     tempTable = FALSE,
                                     progressBar = FALSE)
    }
    DatabaseConnector::executeSql(connection, "COMMIT;", progressBar = FALSE, reportOverallTime = FALSE)
    bulkUploadTable(connection = connection,
                    connectionDetails = connectionDetails,
                    schema = schema,
                    data,
                    tableName = tableName)
  }
}

bulkUploadTable <- function(connection,
                            connectionDetails,
                            schema,
                            data,
                            tableName) {
  startTime <- Sys.time()

  tempFile <- tempfile(fileext = ".csv")
  readr::write_excel_csv(data, tempFile)
  on.exit(unlink(tempFile))

  # For backwards compatibility with older versions of DatabaseConnector:
  if (is(connectionDetails$server, "function")) {
    hostServerDb <- strsplit(connectionDetails$server(), "/")[[1]]
    port <- connectionDetails$port()
    user <- connectionDetails$user()
    password <- connectionDetails$password()
  } else {
    hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]
    port <- connectionDetails$port
    user <- connectionDetails$user
    password <- connectionDetails$password
  }

  startTime <- Sys.time()

  if (.Platform$OS.type == "windows") {
    winPsqlPath <- Sys.getenv("POSTGRES_PATH")
    command <- file.path(winPsqlPath, "psql.exe")
    if (!file.exists(command)) {
      stop("Could not find psql.exe in ", winPsqlPath)
    }
  } else {
    command <- "psql"
  }
  if (is.null(port)) {
    port <- 5432
  }
  headers <- paste0("(", paste(colnames(data), collapse = ","), ")")
  connInfo <- sprintf("host='%s' port='%s' dbname='%s' user='%s' password='%s'", hostServerDb[[1]], port, hostServerDb[[2]], user, password)
  copyCommand <- paste(shQuote(command),
                       "-d \"",
                       connInfo,
                       "\" -c \"\\copy", paste(schema, tableName, sep = "."),
                       headers,
                       "FROM", shQuote(tempFile),
                       "NULL 'NA' DELIMITER ',' CSV HEADER;\"")

  countBefore <- countRows(connection, paste(schema, tableName, sep = "."))
  result <- base::system(copyCommand)
  countAfter <- countRows(connection, paste(schema, tableName, sep = "."))

  if (result != 0) {
    stop("Error while bulk uploading data, psql returned a non zero status. Status = ", result)
  }
  if (countAfter - countBefore != nrow(data)) {
    stop(paste("Something went wrong when bulk uploading. Data has", nrow(data), "rows, but table has", (countAfter - countBefore), "new records"))
  }

  delta <- Sys.time() - startTime
  writeLines(paste("Bulk load to PostgreSQL took", signif(delta, 3), attr(delta, "units")))
}

countRows <- function(connection, sqlTableName) {
  sql <- "SELECT COUNT(*) FROM @table"
  count <- renderTranslateQuerySql(connection = connection,
                                   sql  = sql,
                                   table = sqlTableName)
  return(count[1, 1])
}
