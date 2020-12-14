# This file contains code to be used by the study coordinator to download the files from the SFTP server, and to upload them to the results database.
library(ScyllaEstimation)
library(OhdsiSharing)

allDbsFolder <- "s:/ScyllaEstimation/AllDbs"
# dir.create(allDbsFolder)

# Download files from SFTP server -----------------------------------------------------------------
connection <- sftpConnect(privateKeyFileName = "c:/home/keyfiles/study-coordinator-scylla",
                          userName = "study-coordinator-scylla")

connection <- sftpConnect(privateKeyFileName = "c:/home/keyfiles/study-coordinator-covid19.dat",
                          userName = "study-coordinator-covid19")



# sftpMkdir(connection, "estimation")

sftpCd(connection, "estimation")
sftpCd(connection, "SCYLLAESTIMATION")
files <- sftpLs(connection)
files

sftpGetFiles(connection, files$fileName, localFolder = allDbsFolder)

# DANGER!!! Remove files from server:
sftpRm(connection, files$fileName)

sftpDisconnect(connection)


# Test results locally -----------------------------------------------------------------------------------


# Synthesize results across databases --------------------------------------------------------------------
maExportFolder <- "s:/ScyllaEstimation/MetaAnalysis"
synthesizeResults(allDbsFolder = allDbsFolder, maExportFolder = maExportFolder, maxCores = 10)
file.copy(file.path(maExportFolder, "Results_MetaAnalysis.zip"), file.path(allDbsFolder, "Results_MetaAnalysis.zip"))

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
# createResultsDataModel(connectionDetails, schema)

# # After the tables have been created:
# sql <- "grant select on all tables in schema scylla_estimation to scylla_ro_grp;"
#
# # Next time, before creating tables:
# sql <- "grant usage on schema scylla_estimation to group scylla_ro_grp;
# alter default privileges in schema scylla_estimation grant select on tables to group scylla_ro_grp;
# alter default privileges in schema scylla_estimation grant all on tables to group scylla_rw_grp;"
# connection <- connect(connectionDetails)
# executeSql(connection, sql)
# disconnect(connection)

# Upload data
# allDbsFolder <- "s:/ScyllaEstimation/OptumEhr/export"

uploadedFolder <- file.path(allDbsFolder, "uploaded")

zipFilesToUpload <- list.files(path = allDbsFolder,
                               pattern = ".zip",
                               recursive = FALSE)

for (i in (1:length(zipFilesToUpload))) {
  uploadResultsToDatabase(connectionDetails = connectionDetails,
                          schema = schema,
                          zipFileName = file.path(allDbsFolder, zipFilesToUpload[i]))
  # Move to uploaded folder:
  file.rename(file.path(allDbsFolder, zipFilesToUpload[i]), file.path(uploadedFolder, zipFilesToUpload[i]))
}

