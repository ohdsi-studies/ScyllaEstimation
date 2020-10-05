Executing the study package
===========================================================================================

**NOTE**: This guide assumes you have performed the steps in the [study package setup guide](StudyPackageSetup.md).

This guide will take you through the process of running the study to produce cohort diagnostics and the characterization results.

## How to Run the Study
0. TODO: Run `ScyllaCharacterization`

1. In `R`, you will build an `.Renviron` file. An `.Renviron` is an R environment file that sets variables you will be using in your code. It is encouraged to store these inside your environment so that you can protect sensitive information. Below are brief instructions on how to do this:

````
# The code below makes use of R environment variables (denoted by "Sys.getenv(<setting>)") to
# allow for protection of sensitive information. If you'd like to use R environment variables stored
# in an external file, this can be done by creating an .Renviron file in the root of the folder
# where you have cloned this code. For more information on setting environment variables please refer to:
# https://stat.ethz.ch/R-manual/R-devel/library/base/html/readRenviron.html
#
# Below is an example .Renviron file's contents: (please remove)
# the "#" below as these too are interprted as comments in the .Renviron file:
#
#    DBMS = "postgresql"
#    DB_SERVER = "database.server.com"
#    DB_PORT = 5432
#    DB_USER = "database_user_name_goes_here"
#    DB_PASSWORD = "your_secret_password"
#    ANDROMEDA_TEMP_FOLDER = "E:/andromeda"
#
# The following describes the settings
#    DBMS, DB_SERVER, DB_PORT, DB_USER, DB_PASSWORD := These are the details used to connect
#    to your database server. For more information on how these are set, please refer to:
#    http://ohdsi.github.io/DatabaseConnector/
#
#    ANDROMEDA_TEMP_FOLDER = A directory where temporary files used by the Andromeda package are stored while running.
#
# Once you have established an .Renviron file, you must restart your R session for R to pick up these new
# variables.
````

2. Now you have set-up your environment, you can use the following `R` script to load in your library and configure your environment connection details:

```
library(ScyllaEstimation)

# Optional: specify where the temporary files (used by the Andromeda package) will be created:
andromedaTempFolder <- if (Sys.getenv("ANDROMEDA_TEMP_FOLDER") == "") "~/andromedaTemp" else Sys.getenv("ANDROMEDA_TEMP_FOLDER")
options(andromedaTempFolder = andromedaTempFolder)

# Details for connecting to the server:
dbms = Sys.getenv("DBMS")
user <- if (Sys.getenv("DB_USER") == "") NULL else Sys.getenv("DB_USER")
password <- if (Sys.getenv("DB_PASSWORD") == "") NULL else Sys.getenv("DB_PASSWORD")
connectionString <- if (Sys.getenv("DB_CONNECTION_STRING") == "") NULL else Sys.getenv("DB_CONNECTION_STRING")
server = Sys.getenv("DB_SERVER")
port = Sys.getenv("DB_PORT")
# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

if (!is.null(connectionString)) {
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  connectionString = connectionString,
                                                                  user = user,
                                                                  password = password)

} else {
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  server = server,
                                                                  user = user,
                                                                  password = password,
                                                                  port = port)

}

````

3. Next you will need to specify the database ID, name and description for your CDM as shown below. This information is used in the Shiny results viewer to identify your database. In addition, you will need to specify the database schema that holds your CDM information and a schema that can be used to write results. The user account set in the step above will need read-only access to the `cdmDatabaseSchema` and the ability to create tables & insert data into the `cohortDatabaseSchema`.

Additionally, the `minCellCount` variable below is used to censor any statstics that are below the value specified which by default is 5.

````
# Details specific to the database:
databaseId <- "SIDIAP"
databaseName <- "Information System for Research in Primary Care (SIDIAP)"
databaseDescription <- "The Information System for Research in Primary Care (SIDIAP; www.sidiap.org) is a primary care records database that covers approximately 7 million people, equivalent to an 80% of the population of Catalonia, North-East Spain. Healthcare is universal and tax-payer funded in the region, and primary care physicians are gatekeepers for all care and responsible for repeat prescriptions."

# Details for connecting to the CDM and storing the results
cdmDatabaseSchema <- "cdm_health_verity_v1282_2"
cohortDatabaseSchema <- "cdm_health_verity_v1282_2"
cohortTable <- paste0("AS_ScyllaChar_", databaseId)
cohortStagingTable <- paste0(cohortTable, "_stg")
featureSummaryTable <- paste0(cohortTable, "_smry")
minCellCount <- 5

````

4. Set the file location where you will hold the study results. Please note that the `projectRootFolder` must match the location specified used in the [study package setup guide](STUDY-PACKAGE-SETUP.md). The additional variables below the `setwd(outputFolder)` should be left.

````
# Set the folder for holding the study output
projectRootFolder <- "E:/ScyllaEstimation"
outputFolder <- file.path(projectRootFolder, databaseId)
if (!dir.exists(outputFolder)) {
  dir.create(outputFolder)
}
setwd(outputFolder)

# Details for running the study.
useBulkCharacterization <- TRUE
cohortIdsToExcludeFromExecution <- c()
cohortIdsToExcludeFromResultsExport <- NULL
````

5. You can now run the estimation package. This step is designed to take advantage of incremental building. This means if the job fails, the R package will start back up where it left off. This package has been designed to be computationally efficient. In SIDIAP data, this package took approximately 3 hours. In Janssen data, it ran in under an hour. Package runtime will vary based on your infrastructure but it should be significantly faster than your prior CohortDiagnostic run.

In your `R` script, you will use the following code:
````
# Use this to run the study. The results will be stored in a zip file called
# 'Results_<databaseId>.zip in the outputFolder.
execute(connectionDetails = connectionDetails,
         cdmDatabaseSchema = cdmDatabaseSchema,
         cohortDatabaseSchema = cohortDatabaseSchema,
         cohortStagingTable = cohortStagingTable,
         cohortTable = cohortTable,
         featureSummaryTable = featureSummaryTable,
         oracleTempSchema = cohortDatabaseSchema,
         exportFolder = outputFolder,
         databaseId = databaseId,
         databaseName = databaseName,
         databaseDescription = databaseDescription,
         cohortIdsToExcludeFromExecution = cohortIdsToExcludeFromExecution,
         cohortIdsToExcludeFromResultsExport = cohortIdsToExcludeFromResultsExport,
         incremental = TRUE,
         useBulkCharacterization = useBulkCharacterization,
         minCellCount = minCellCount)
````

7. TODO

Please send an email to [James Weaver](mailto:jweave17@its.jnj.com) to notify you have dropped results in the folder.
