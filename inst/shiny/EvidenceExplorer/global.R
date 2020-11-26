library(dplyr)
source("DataPulls.R")
source("PlotsAndTables.R")

# shinySettings <- list(dataFolder = "d:/ScyllaEstimation/Premier/shinyData", blind = TRUE)

designs <- data.frame(
  description = c("Admission to intensive services; washout; -1 end days; ",
                  "Admission to intensive services; no washout; 0 end days; ",
                  "During hospitalization to intensive services; washout; -1 end days; ",
                  "Post testing to hospitalization; washout; 0 end days; "),
  label = c("Treatment administered on the date of admission of hospitalization and prior to intensive services and 365d prior observation",
            "Treatment administered on the date of admission of hospitalization and prior to intensive services with no prior observation",
            "Treatment administered during hospitalization and prior to intensive services and 365d prior observation",
            "Persons with a COVID-19 diagnosis record or a SARS-CoV-2 positive test prior to inpatient visit or intensive services and 365d prior observation"),
  remove = c(" with Treatment administered on the date of admission of hospitalization and prior to intensive services and 365d prior observation",
             " with Treatment administered on the date of admission of hospitalization and prior to intensive services with no prior observation",
             " with Treatment administered during hospitalization and prior to intensive services and 365d prior observation",
             " with Persons with a COVID-19 diagnosis record or a SARS-CoV-2 positive test prior to inpatient visit or intensive services and 365d prior observation"),
  idMask = c(100, 200, 300, 400)
)

dataFolder <- shinySettings$dataFolder
blind <- shinySettings$blind
connection <- NULL
positiveControlOutcome <- NULL

splittableTables <- c("covariate_balance", "preference_score_dist", "kaplan_meier_dist")

files <- list.files(dataFolder, pattern = ".rds")

# Find part to remove from all file names (usually databaseId):
databaseFileName <- files[grepl("^database", files)]
removeParts <- paste0(gsub("database", "", databaseFileName), "$")

# Remove data already in global environment:
for (removePart in removeParts) {
  tableNames <- gsub("_t[0-9]+_c[0-9]+$", "", gsub(removePart, "", files[grepl(removePart, files)]))
  camelCaseNames <- SqlRender::snakeCaseToCamelCase(tableNames)
  camelCaseNames <- unique(camelCaseNames)
  camelCaseNames <- camelCaseNames[!(camelCaseNames %in% SqlRender::snakeCaseToCamelCase(splittableTables))]
  suppressWarnings(rm(list = camelCaseNames))
}

# Load data from data folder. R data objects will get names derived from the filename:
loadFile <- function(file, removePart) {
  tableName <- gsub("_t[0-9]+_c[0-9]+$", "", gsub(removePart, "", file))
  camelCaseName <- SqlRender::snakeCaseToCamelCase(tableName)
  if (!(tableName %in% splittableTables)) {
    newData <- readRDS(file.path(dataFolder, file))
    colnames(newData) <- SqlRender::snakeCaseToCamelCase(colnames(newData))
    if (exists(camelCaseName, envir = .GlobalEnv)) {
      existingData <- get(camelCaseName, envir = .GlobalEnv)
      newData <- rbind(existingData, newData)
      newData <- unique(newData)
    }
    assign(camelCaseName, newData, envir = .GlobalEnv)
  }
  invisible(NULL)
}
for (removePart in removeParts) {
  lapply(files[grepl(removePart, files)], loadFile, removePart)
}

tcos <- unique(cohortMethodResult[, c("targetId", "comparatorId", "outcomeId")])
tcos <- tcos[tcos$outcomeId %in% outcomeOfInterest$outcomeId, ]
validExposureIds <- unique(c(tcos$targetId, tcos$comparatorId))

vecDesign <- Vectorize(ScyllaEstimation:::getDesign)

exposureOfInterest$design <- vecDesign(exposureOfInterest$exposureId)

exposureOfInterest <- exposureOfInterest %>% 
  inner_join(designs %>% 
               select(idMask, remove), by = c("design" = "idMask")) %>%
    rowwise() %>% 
  mutate(shortName = sub(pattern = remove, replacement = "", x = exposureName)) %>%
  filter(exposureId %in% validExposureIds)

designs <- designs %>%
  filter(idMask %in% unique(exposureOfInterest$design))

cohortMethodAnalysis <- cohortMethodAnalysis %>% 
  mutate(design = floor(analysisId / 100) * 100) %>%
  inner_join(designs %>% 
               select(idMask, description) %>% 
               rename(remove = description), 
             by = c("design" = "idMask")) %>%
  rowwise() %>% 
  mutate(shortDescription = sub(pattern = remove, replacement = "", x = description))

