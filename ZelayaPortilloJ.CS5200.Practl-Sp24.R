#' ---
#' title: "Analysis of Wildlife Strikes to Aircraft"
#' author: "Julissa Zelaya Portillo"
#' date: "Spring 2024"
#' output:
#'   pdf_document: default
#'   html_notebook: default
#' subtitle: Practicum I CS5200
#' ---
#' 
#' The collision between a wildlife bird and an aircraft poses risks to aircraft
#' safety. To analyze the impact of bird strikes on aircraft safety, the Federal
#' Aviation Administration (FAA) has provided instances of impact and the measured
#' effect on the flight through an eleven-year period. 
#' 
#' ## Creating the Database Schema
#' 
#' Analysis is performed through MySQL queries for the before mentioned data set
#'  provided by the FAA. The following packages allow the querying and analysis
#' of the MYSQL database through R: 'dplyr', 'RMySQL', 'DBI', and 'readr'.
#' 
## ----library, warning=FALSE, message=FALSE, echo=FALSE---------------------------------------------------------------------------------------------------------------------------------
# Install and load packages suited for AWS MySQL RDS
# Package names
packages <- c("dplyr", "RMySQL", "DBI", "readr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

#' 
#' The connection to the 'BirdStrikes' MYSQL database is provided by AWS through
#' the specified host, port, and access credentials.
#' 
## ----dbCredentials, message=FALSE, warning=FALSE, echo=FALSE---------------------------------------------------------------------------------------------------------------------------
# Connection variables to access AWS RDS
host  ='birdstrikes.c3ay0s2y4hil.us-east-1.rds.amazonaws.com'
port = 3306
dbname = 'BirdStrikes'

# Username and password to access AWS RDS
user  ='zelayaportilloj'
password='Huskies123!'

#' 
## ----connectDB, message=FALSE, warning=FALSE, echo=FALSE-------------------------------------------------------------------------------------------------------------------------------
# Establish connection to AWS DB
con = dbConnect(RMySQL::MySQL(),
                dbname=dbname,
                host=host,
                port=port,
                user=user,
                password=password)

#' 
#' To prevent any existing constraints from causing conflicting data, all tables
#' and foreign constraints are dropped to being anew.
#' 
## ----dropTables, echo=FALSE, message=FALSE, warning=FALSE------------------------------------------------------------------------------------------------------------------------------
# Drop tables prior to run
# Define database tables
db_tables <- c("airports", "flights", "conditions", "strikes")

# Ensure that foreign key checks are disabled prior to table drops
# Function to drop tables prior to run and re-enable foreign key checks
drop_tables <- function(con, tables) {
  # Disable foreign key checks
  dbSendQuery(con, "SET FOREIGN_KEY_CHECKS = 0;")
  
  # Drop each table if it exists
  for (table in tables) {
    # Construct query to drop table if it exists
    query <- paste("DROP TABLE IF EXISTS", table, "CASCADE")
    # Execute drop query
    dbSendQuery(con, query)
  }
  
  # Re-enable foreign key checks
  dbSendQuery(con, "SET FOREIGN_KEY_CHECKS = 1;")
}

drop_schema <- drop_tables(con, db_tables)

#' 
#' The four tables to be created as part of the 'BirdStrikes' database schema are
#' airports, conditions, flights, and strikes. Appropriate primary, foreign keys,
#' and default values are defined at this point during table creation. Each query
#' is then sent to the database cloud to create the corresponding table. 
#' 
## ----defineSchemas, warning=FALSE, message=FALSE, echo=FALSE---------------------------------------------------------------------------------------------------------------------------
# Define the schema for the airports table
# Note: airports is created first to avoid instance of hanging FK reference
tableAirports <- "
CREATE TABLE IF NOT EXISTS airports (
    aid INTEGER PRIMARY KEY AUTO_INCREMENT,
    airportName TEXT DEFAULT ('Unknown'),
    airportState TEXT DEFAULT ('Unknown'),
    airportCode TEXT DEFAULT ('ZZZ')
);
"
# Send query to create the airports table
resultTableAirports <- dbSendQuery(con, tableAirports)


# Define the schema for the conditions table
tableConditions <- "
CREATE TABLE IF NOT EXISTS conditions (
    cid INTEGER PRIMARY KEY AUTO_INCREMENT,
    sky_condition VARCHAR(50) UNIQUE,
    explanation VARCHAR(50)
);
"
# Send query to create the conditions table
resultTableConditions <- dbSendQuery(con, tableConditions)


# Define the schema for the flights table
# NOTE: 'fid' is no longer a foreign key as this uses the 'rid' column
tableFlights <- "
CREATE TABLE IF NOT EXISTS flights (
    fid INTEGER PRIMARY KEY,
    date DATE,
    originAirport INTEGER,
    airlineName TEXT DEFAULT ('Unknown'),
    aircraftType TEXT DEFAULT ('Airplane'),
    isHeavy BOOLEAN,
    FOREIGN KEY (originAirport) REFERENCES airports (aid) 
    ON UPDATE CASCADE ON DELETE CASCADE
);
"
# Send query to create the flights table
resultTableFlights <- dbSendQuery(con, tableFlights)


# Define the schema for the strikes table
# NOTE: 'fid' is no longer a foreign key as this uses the 'rid' column
tableStrikes <- "
CREATE TABLE IF NOT EXISTS strikes (
    sid INTEGER PRIMARY KEY AUTO_INCREMENT,
    fid INTEGER,
    numbirds INTEGER DEFAULT ('1'),
    impact TEXT,
    damage BOOLEAN,
    altitude INTEGER CHECK (altitude >= 0),
    conditions INTEGER,
    model TEXT DEFAULT ('Unknown'),
    flight_phase TEXT,
    wildlife_size TEXT,
    pilot_warned_flag BOOLEAN,
    FOREIGN KEY (conditions) REFERENCES conditions (cid)
    ON UPDATE CASCADE ON DELETE CASCADE
);
"
# Send query to create the strikes table
resultTableStrikes <- dbSendQuery(con, tableStrikes)

#' 
#' 
#' ## Test the Database Schema
#' 
#' There are several methods to test the appropriate creation of the database schema.
#' The use of "SHOW CREATE TABLE" confirms that all table creations, keys, and
#' constraints are now seen in the MYSQL database. This metadata reveals that the
#' created schema for the 'BirdStrikes' table includes all appropriate PK, FK,
#' and constraints across all four tables within the needed schema .
#' 
## ----testTableInfo, warning=FALSE, message=FALSE, echo=FALSE, eval=FALSE---------------------------------------------------------------------------------------------------------------
## # Function to retrieve table definition using SHOW CREATE TABLE
## table_definition <- function(con, table_names) {
##   # Iterate over each table
##   for (table in table_names) {
##     # Construct the SHOW CREATE TABLE query
##     query <- paste("SHOW CREATE TABLE", table)
## 
##     # Execute query and print result
##     result <- dbGetQuery(con, query)
##     cat(result$`Create Table`, "\n\n")
##   }
## }
## 
## # Call the function to retrieve table definition for each table
## table_definition(con, db_tables)

#' 
#' Sample data is provided for all four tables to confirm the working condition of
#' sending data to the database cloud. 
#' 
## ----insertSample, message=FALSE, warning=FALSE, echo=FALSE, eval=FALSE----------------------------------------------------------------------------------------------------------------
## # Insert sample data into the airports table
## sample_airports <- data.frame(
##   airportName = c("Airport 1", "Airport 2", "Airport 3"),
##   airportState = c("State 1", "State 2", "State 3"),
##   airportCode = c("AAA", "BBB", "CCC")
## )
## sampleAirports <- dbWriteTable(con, "airports", sample_airports, append = TRUE,
##                                row.names = FALSE)
## 
## # Insert sample data into the conditions table
## sample_conditions <- data.frame(
##   sky_condition = c("Sky Condition 1", "Sky Condition 2", "Sky Condition 3"),
##   explanation = c("Explanation 1", "Explanation 2", "Explanation 3")
## )
## sampleCond <- dbWriteTable(con, "conditions", sample_conditions, append = TRUE,
##                            row.names = FALSE)
## 
## # Insert sample data into the flights table
## sample_flights <- data.frame(
##   fid = c(1, 2, 3),
##   date = c("2024-01-01", "2024-02-02", "2024-03-03"),
##   originAirport = c(1, 2, 3),
##   airlineName = c("Airline 1", "Airline 2", "Airline 3"),
##   aircraftType = c("Aircraft 1", "Aircraft 2", "Aircraft 3"),
##   isHeavy = c(TRUE, FALSE, TRUE)
## )
## sampleFlights <- dbWriteTable(con, "flights", sample_flights, append = TRUE,
##                               row.names = FALSE)
## 
## # Insert sample data into the strikes table
## sample_strikes <- data.frame(
##   fid = c(1, 2, 3),
##   numbirds = c(5, 10, 15),
##   impact = c("Impact 1", "Impact 2", "Impact 3"),
##   damage = c(TRUE, FALSE, TRUE),
##   altitude = c(1000, 2000, 3000),
##   conditions = c(1, 2, 3),
##   model = c("Model 1", "Model 2", "Model 3"),
##   flight_phase = c("Phase 1", "Phase 2", "Phase 3"),
##   wildlife_size = c("Size 1", "Size 2", "Size 3"),
##   pilot_warned_flag = c(TRUE, FALSE, TRUE)
## )
## sampleStrikes <- dbWriteTable(con, "strikes", sample_strikes, append = TRUE,
##                                row.names = FALSE)

#' 
#' Upon inserting sample data, queries are created to print that sample data from
#' each table and confirm that the sample insertions were successful. This also
#' provides general information on the number of variables that are expected in 
#' each table along with corresponding data types for each column.
#' 
## ----testTableStructure, warning=FALSE, message=FALSE, echo=FALSE, eval=FALSE----------------------------------------------------------------------------------------------------------
## # Test that the column structures for each table were created
## test_schema <- function(con) {
##   # Define test queries
##   test_schema <- c(
##     "SELECT * FROM airports;",
##     "SELECT * FROM flights;",
##     "SELECT * FROM conditions;",
##     "SELECT * FROM strikes;"
##   )
## 
##   # Execute each test query and print the structure of the resulting data frame
##   for (query in test_schema) {
##     result <- dbGetQuery(con, query)
##     print(str(result))
##   }
## }
## test_schema(con)

#' 
#' This sample data is then removed from all four tables. Previously created
#' functions such as 'test_schema' can be ran again to confirm that these tables
#' have been cleared. 
#' 
## ----removeSample, message=FALSE, warning=FALSE, echo=FALSE, eval=FALSE----------------------------------------------------------------------------------------------------------------
## # Function to remove sample data from all tables
## remove_sample_data <- function(con) {
##   # Iterate over each table and remove sample data
##   for (table in db_tables) {
##     # Construct the DELETE query
##     query <- paste("DELETE FROM", table)
## 
##     # Execute the DELETE query
##     dbSendQuery(con, query)
##   }
## }
## 
## # Call the function to remove sample data
## remove_sample_data(con)

#' 
#' 
#' ## Structure of Source Data
#' 
#' The source of the data, as mentioned, is provided by the FAA in regard to
#' reported bird strikes between wildlife and aircraft. This provided data is read
#' into the 'bds.raw' data frame to prepare for further data transformations. 
#' 
## ----sourceData, warning=FALSE, message=FALSE, echo=FALSE------------------------------------------------------------------------------------------------------------------------------
# Load data from CSV source
bds.raw <- read.csv("BirdStrikesData-V3.csv", 
                    header = TRUE, stringsAsFactors = FALSE)

#' 
#' It is noted that various columns do not properly conform to their needed data types.
#' These data transformations are handled prior to upload into the database to
#' prevent issues in maintaining referential integrity. This includes the mapping
#' of various string characters to a boolean data type, removal of characters within
#' integer values, and converting string characters to an appropriate date format.
#' 
## ----dataTransformation, warning=FALSE, message=FALSE, echo=FALSE----------------------------------------------------------------------------------------------------------------------
# Create a function that converts columns to Boolean data type
convertBoolean <- function(chr) {
  
  # Use toupper() element-wise to convert all values to uppercase
  chr <- sapply(chr, toupper)
  
  # Use the mapping to convert characters to logical values
  boolean_map <- c("Y" = TRUE, "YES" = TRUE, "DAMAGE" = TRUE, "LOSS" = TRUE,
                   "N" = FALSE, "NO" = FALSE, "NO DAMAGE" = FALSE)
  
  # Use match to find matching values in boolean_map
  boolean <- as.logical(boolean_map[match(chr, names(boolean_map))])
  return(boolean)
}

# Create a function that removes the thousands comma separator
removeThousandSeparator <- function(number) {
  # Remove commas and convert to numeric
  return(as.numeric(gsub(",", "", number)))
}

# Create a function that converts character type to date
formatDate <- function(char_column) {

  # Convert character column to datetime using default time zone
  datetime_column <- as.POSIXct(char_column, format = "%m/%d/%Y")
  
  # Return the datetime column
  return(datetime_column)
}

#' 
#' The transformation are ran on the 'bds.raw' data frame into the 'bds.transformed'
#' data frame.
#' 
## ----runTransformations, warning=FALSE, message=FALSE, echo=FALSE----------------------------------------------------------------------------------------------------------------------
# Run data transformation based on previous functions
# Create a copy of bds.raw
bds.transformed <- bds.raw

# Convert specified columns to boolean
bds.transformed$damage <- convertBoolean(bds.raw$damage)
bds.transformed$pilot_warned_flag <- convertBoolean(bds.raw$pilot_warned_flag)
bds.transformed$heavy_flag <- convertBoolean(bds.raw$heavy_flag)

# Column 'altitude_ft' needs commas removed from integers
bds.transformed$altitude_ft <- removeThousandSeparator(bds.raw$altitude_ft)

# Column 'flight_date' needs to be changed to a date type
bds.transformed$flight_date <- formatDate(bds.raw$flight_date)

#' 
#' Once the appropriate data types are confirmed, the columns of the source csv file 
#' are mapped to the corresponding database table columns. This is an important step
#' as both the csv file and database have differing names for equivalent columns. The
#' columns are then mapped to a dataframe sorted by database table. As the columns
#' are mapped for each table, the 'dbWriteTable()' function appends the data to
#' populate this into the database while honoring the existing schema of the database. 
#' 
## ----dataMapping, warning=FALSE, message=FALSE, echo=FALSE-----------------------------------------------------------------------------------------------------------------------------
# Define the mapping of columns between data frame and data base
# Airports
map_airports <- c("aid" = NULL, "airportName" = "airport",
                  "airportState" = "origin", "airportCode" = NULL)
# Rename columns for airports table
bds.transformed_airports <- dplyr::select(bds.transformed, all_of(map_airports))

# Insert data into the airports table
write_airports <- dbWriteTable(con, "airports", bds.transformed_airports, 
                               append = TRUE, overwrite = FALSE, row.names = FALSE)

# Flights
map_flights <- c("fid" = "rid", "date" = "flight_date", "airlineName" = "airline",
                 "aircraftType" = "aircraft", "isHeavy" = "heavy_flag")
# Rename columns for flights table
bds.transformed_flights <- dplyr::select(bds.transformed, all_of(map_flights))
# Insert data into the flights table
write_flights <- dbWriteTable(con, "flights", bds.transformed_flights, 
                              append = TRUE, overwrite = FALSE, row.names = FALSE)

# Conditions
map_conditions <- c("cid" = NULL, "sky_condition" = "sky_conditions", 
                    "explanation" = NULL)
# Rename columns for conditions table
bds.transformed_conditions <- dplyr::select(bds.transformed, all_of(map_conditions))
# Insert data into the conditions table
write_conditions <- dbWriteTable(con, "conditions", bds.transformed_conditions, 
                                 append = TRUE, overwrite = FALSE, row.names = FALSE)

# Strikes
map_strikes <- c("sid" = NULL, "fid" = "rid", "numbirds" = NULL,"impact" = "impact",
                 "damage" = "damage","altitude" = "altitude_ft", "model" = "model",
                 "flight_phase" = "flight_phase", "wildlife_size" = "wildlife_size",
                 "pilot_warned_flag" = "pilot_warned_flag")
# Rename columns for strikes table
bds.transformed_strikes <- dplyr::select(bds.transformed, all_of(map_strikes))
# Insert data into the strikes table
write_strikes <- dbWriteTable(con, "strikes", bds.transformed_strikes, 
                              append = TRUE, overwrite = FALSE, row.names = FALSE)

#' 
#' The top five rows of each database table are listed to test that the source
#' csv file was properly loaded into the database cloud. Upon running the test,
#' it is confirmed that the data was properly populated into the correct table.
#' 
## ----testPopulateData, warning=FALSE, message=FALSE, echo=FALSE, eval=FALSE------------------------------------------------------------------------------------------------------------
## # Function to retrieve top five data for specified tables
## top_five_data <- function(con, tables) {
##   # Iterate over each table
##   for (table in tables) {
##     # Construct query to retrieve top five data
##     query <- paste("SELECT * FROM", table)
## 
##     # Execute query and print result
##     result <- dbGetQuery(con, query)
##     print(result)
##   }
## }
## 
## # Call the function with the provided database tables
## top_five_data(con, db_tables)
## # Call previous function to look at FK mappings
## table_definition(con, db_tables)

#' 
#' ## Top Airports with Strikes
#' In analyzing the data from the 'BirdStrikes' database, a SQL query against the
#' database finds the top 10 states with the greatest number of bird strike incidents.
#' 
## SELECT airportState AS State, COUNT(*) AS BirdStrikeIncidents
## FROM strikes s
## JOIN flights f ON s.fid = f.fid
## JOIN airports a ON f.originAirport = a.aid
## GROUP BY airportState
## ORDER BY BirdStrikeIncidents DESC
## LIMIT 10;
#' 
#' 
#' ## Analysis by Airline
#' In further analyzing the data from the 'BirdStrikes' database, a SQL query against
#' the database finds the airlines that had an above average number of bird strike
#' incidents. 
#' 
## SELECT airlineName AS Airline, COUNT(*) AS NumberBirdStrikes
## FROM strikes s
## JOIN flights f ON s.fid = f.fid
## GROUP BY airlineName
## HAVING NumberBirdStrikes > (SELECT AVG(incident_count) FROM (SELECT COUNT(*)
## AS incident_count FROM strikes s JOIN flights f ON s.fid = f.fid
## GROUP BY airlineName) AS avg_table)
## ORDER BY NumberBirdStrikes DESC;
#' 
#' ## Analysis by Month
#' A SQL query against the 'BirdStrikes' database finds the total number of wildlife
#' strikes by month. The first five months are shown as well as wildlife strikes
#' that were provided with no indicated date of impact. 
#' 
## ----analysisMonth, echo=FALSE---------------------------------------------------------------------------------------------------------------------------------------------------------
# Execute the SQL query and store the result in a data frame
analysisMonth <- dbGetQuery(con, "
SELECT DATE_FORMAT(f.date, '%M') AS Month,
COUNT(*) AS WildlifeStrikesByMonth
FROM strikes s
JOIN flights f ON s.fid = f.fid
GROUP BY MONTH(f.date)
ORDER BY MONTH(f.date);")

head(analysisMonth, 6)
# NOTE: NA values are present due to blank date values

#' 
#' 
#' ## Trend by Month
#' Based on the data frame created in the previous query, a visualization of the
#' number of strikes or birds by month. This visual provides the quick identification
#' that August experienced more strikes of wildlife birds than any other month. 
#' 
## ----trendMonth, echo=FALSE------------------------------------------------------------------------------------------------------------------------------------------------------------
# NOTE: numbirds = the number of strikes
# Create a vertical column chart
barplot(analysisMonth$WildlifeStrikesByMonth, 
        names.arg = analysisMonth$Month,
        xlab = "Month",
        ylab = "Number of Wildlife Strikes/Birds",
        main = "Trend of Wildlife Strikes: Trend by Month",
        ylim = c(0, max(analysisMonth$WildlifeStrikesByMonth)),
        las = 2,
        cex.names = 0.8,
        col = "red")


#' 
#' In the event that a wildlife incident needs to be removed, a stored procedure is
#' created to remove the incidence. An audit log is also created to document the
#' removal modification along with the time, date, and corresponding primary key. 
#' 
## ----createAuditTable, message=FALSE, warning=FALSE, echo=FALSE------------------------------------------------------------------------------------------------------------------------
# Define the schema for the audit_log table
table_audit_log <- "
CREATE TABLE IF NOT EXISTS audit_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    modification_type VARCHAR(50),
    table_name VARCHAR(50),
    timestamp TIMESTAMP,
    strike_id INT
);
"
# Send query to create the audit_log table
resultTableAuditLog <- dbSendQuery(con, table_audit_log)


#' 
#' The removal of the incidence is performed through the specification of the
#' appropriate 'sid' primary key value of the strikes table. The log will reflect
#' all removal requests, including duplicate requests. 
#' 
## ----storedProcedure, warning=FALSE, message=FALSE, echo=FALSE-------------------------------------------------------------------------------------------------------------------------
# Define the stored procedure to remove a strike and log the removal
remove_strike_procedure <- "
CREATE PROCEDURE IF NOT EXISTS RemoveStrike(IN strike_id INT)
BEGIN
    -- Delete the strike from the strikes table
    DELETE FROM strikes WHERE sid = strike_id;
    
    -- Insert a record into the audit_log table
    INSERT INTO audit_log (modification_type, table_name, timestamp, strike_id)
    VALUES ('Removal', 'strikes', NOW(), strike_id);
END"
# Send query to create the stored procedure
removeStrike <- dbSendQuery(con, remove_strike_procedure)

#' 
## ----removeStrike, message=FALSE, warning=FALSE, echo=FALSE----------------------------------------------------------------------------------------------------------------------------
# Function to remove a strike by calling the stored procedure
remove_strike <- function(con, sid_value) {
  query <- paste("CALL RemoveStrike(", sid_value, ")", sep = "")
  dbSendQuery(con, query)
}

# Example: Call the stored procedure to remove a strike with SID 123
call_strike_removal <- remove_strike(con, 123)

# Check the audit log to verify the removal operation
audit_log <- dbGetQuery(con, "SELECT * FROM audit_log")
head(audit_log)


#' 
#' ## Close the Database
#' At the conclusion of the creation of the 'BirdStrikes' database and analysis,
#' the connection to the database is disconnected. 
#' 
## ----disconnectDB, warning=FALSE, message=FALSE, echo=FALSE----------------------------------------------------------------------------------------------------------------------------
# Disconnect connection to AWS DB
disconnect <- dbDisconnect(con)

#' 
