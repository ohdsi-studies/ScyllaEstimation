# This file contains code to be used by the study coordinator to download the files from the SFTP server, and to upload them to the results database.

library(OhdsiSharing)

localFolder <- "s:/ScyllaEstimation/AllDbs"
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


# Synthesize results across databases --------------------------------------------------------------------
synthesizeResults(allDbsFolder = localFolder)

# Upload results to database -----------------------------------------------------------------------
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("scyllaServer"),
                                                            keyring::key_get("scyllaDatabase"),
                                                            sep = "/"),
                                             user = keyring::key_get("scyllaUser"),
                                             password = keyring::key_get("scyllaPassword"))
schema <- "scylla_estimation"

# Do this only once!
createResultsDataModel(connectionDetails, schema)

# Upload data
allDbsFolder <- "s:/ScyllaEstimation/OptumEhr/export"

zipFilesToUpload <- list.files(path = allDbsFolder,
                               pattern = ".zip",
                               recursive = FALSE)

for (i in (1:length(zipFilesToUpload))) {
  uploadResultsToDatabase(connectionDetails = connectionDetails,
                          schema = schema,
                          zipFileName = file.path(allDbsFolder, zipFilesToUpload[i]))
  # Move to uploaded folder:
  file.rename(file.path(localFolder, zipFilesToUpload[i]), file.path(uploadedFolder, zipFilesToUpload[i]))
}

