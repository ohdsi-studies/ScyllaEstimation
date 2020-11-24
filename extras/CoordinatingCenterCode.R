# This file contains code to be used by the study coordinator to download the files from the SFTP server, and to upload them to the results database.

library(OhdsiSharing)

localFolder <- "s:/ScyllaEstimation"
# dir.create(localFolder)

# Download files from SFTP server -----------------------------------------------------------------
connection <- sftpConnect(privateKeyFileName = "c:/home/keyfiles/study-coordinator-scylla",
                          userName = "study-coordinator-scylla")

# sftpMkdir(connection, "estimation")

sftpCd(connection, "estimation")
files <- sftpLs(connection)
files

sftpGetFiles(connection, files$fileName, localFolder = localFolder)

# DANGER!!! Remove files from server:
sftpRm(connection, files$fileName)

sftpDisconnect(connection)


# Test results locally -----------------------------------------------------------------------------------

# Upload results to database -----------------------------------------------------------------------
# library(DatabaseConnector)
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = paste(Sys.getenv("phenotypeLibraryDbServer"),
#                                                             Sys.getenv("phenotypeLibraryDbDatabase"),
#                                                             sep = "/"),
#                                              port = Sys.getenv("phenotypeLibraryDbPort"),
#                                              user = Sys.getenv("phenotypeLibraryDbUser"),
#                                              password = Sys.getenv("phenotypeLibraryDbPassword"))
# resultsSchema <- Sys.getenv("phenotypeLibraryDbResultsSchema")

# Only the first time:
# CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)

# uploadedFolder <- file.path(localFolder, "uploaded")
# if (!file.exists(uploadedFolder)) {
#   dir.create(uploadedFolder)
# }
# zipFilesToUpload <- list.files(path = localFolder,
#                                pattern = ".zip",
#                                recursive = FALSE)
#
# for (i in (1:length(zipFilesToUpload))) {
#   CohortDiagnostics:: uploadResults(connectionDetails = connectionDetails,
#                                     schema = resultsSchema,
#                                     zipFileName = file.path(localFolder, zipFilesToUpload[i]))
#   # Move to uploaded folder:
#   file.rename(file.path(localFolder, zipFilesToUpload[i]), file.path(uploadedFolder, zipFilesToUpload[i]))
# }

