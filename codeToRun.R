library(Strategus)

# Inputs to run (edit these for your CDM):
# ========================================= #
if (!Sys.getenv("DATABASE_TEMP_SCHEMA") == "") {
  options(sqlRenderTempEmulationSchema = Sys.getenv("DATABASE_TEMP_SCHEMA"))
}

database <- "ducky"

# reference for the connection used by Strategus
connectionDetailsReference <- paste0("covid_3p", database)

# where to save the output - a directory in your environment
outputFolder <- "/home/egill/projects/PredictionPandemicPerformance/output/"

# fill in your connection details and path to driver
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "~/database/database-1M_filtered.duckdb"
)
# A schema with write access to store cohort tables
workDatabaseSchema <- "cohorts"
  
# name of cohort table that will be created for study
cohortTable <- "covid_3p"

# schema where the cdm data is
cdmDatabaseSchema <- "main"

# Aggregated statistics with cell count less than this are removed before sharing results.
minCellCount <- 5


# Location to Strategus modules
# If you've ran Strategus studies before this directory should already exist.
# Note: this environmental variable should be set once for each compute node
Sys.setenv("INSTANTIATED_MODULES_FOLDER" = '/home/egill/modules/')


# =========== END OF INPUTS ========== #

Strategus::storeConnectionDetails(
  connectionDetails = connectionDetails,
  connectionDetailsReference = connectionDetailsReference
)

executionSettings <- Strategus::createCdmExecutionSettings(
  connectionDetailsReference = connectionDetailsReference,
  workDatabaseSchema = workDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
  workFolder = file.path(outputFolder, "strategusWork"),
  resultsFolder = file.path(outputFolder, "strategusOutput"),
  minCellCount = minCellCount
)

json <- paste(readLines('./study_execution_jsons/outpatient_critical_simple_validation.json'), collapse = '\n')
analysisSpecifications <- ParallelLogger::convertJsonToSettings(json)

Strategus::execute(
  analysisSpecifications = analysisSpecifications,
  executionSettings = executionSettings,
  executionScriptFolder = file.path(outputFolder, "strategusExecution"),
  restart = F
)
