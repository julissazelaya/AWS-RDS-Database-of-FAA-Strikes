# Analysis of Wildlife Strikes to Aircraft
The collision between a wildlife bird and an aircraft poses risks to aircraft safety. To analyze the impact of
bird strikes on aircraft safety, the Federal Aviation Administration (FAA) has provided instances of impact
and the measured effect on the flight through an eleven-year period.

## Creating the Database Schema
Analysis is performed through MySQL queries for the before mentioned data set provided by the FAA. The
following packages allow the querying and analysis of the MYSQL database through R: ‘dplyr’, ‘RMySQL’,
‘DBI’, and ‘readr’.
The connection to the ‘BirdStrikes’ MYSQL database is provided by AWS through the specified host, port,
and access credentials.
To prevent any existing constraints from causing conflicting data, all tables and foreign constraints are
dropped to being anew.
The four tables to be created as part of the ‘BirdStrikes’ database schema are airports, conditions, flights,
and strikes. Appropriate primary, foreign keys, and default values are defined at this point during table
creation. Each query is then sent to the database cloud to create the corresponding table.

## Test the Database Schema
There are several methods to test the appropriate creation of the database schema. The use of “SHOW
CREATE TABLE” confirms that all table creations, keys, and constraints are now seen in the MYSQL
database. This metadata reveals that the created schema for the ‘BirdStrikes’ table includes all appropriate
PK, FK, and constraints across all four tables within the needed schema .
Sample data is provided for all four tables to confirm the working condition of sending data to the database
cloud.
Upon inserting sample data, queries are created to print that sample data from each table and confirm that
the sample insertions were successful. This also provides general information on the number of variables
that are expected in each table along with corresponding data types for each column.
This sample data is then removed from all four tables. Previously created functions such as ‘test_schema’
can be ran again to confirm that these tables have been cleared.

## Structure of Source Data
The source of the data, as mentioned, is provided by the FAA in regard to reported bird strikes between
wildlife and aircraft. This provided data is read into the ‘bds.raw’ data frame to prepare for further data
transformations.
It is noted that various columns do not properly conform to their needed data types. These data transformations
are handled prior to upload into the database to prevent issues in maintaining referential integrity.
This includes the mapping of various string characters to a boolean data type, removal of characters within
integer values, and converting string characters to an appropriate date format.
The transformation are ran on the ‘bds.raw’ data frame into the ‘bds.transformed’ data frame.
Once the appropriate data types are confirmed, the columns of the source csv file are mapped to the
corresponding database table columns. This is an important step as both the csv file and database have
differing names for equivalent columns. The columns are then mapped to a dataframe sorted by database
table. As the columns are mapped for each table, the ‘dbWriteTable()’ function appends the data to populate
this into the database while honoring the existing schema of the database.
The top five rows of each database table are listed to test that the source csv file was properly loaded into
the database cloud. Upon running the test, it is confirmed that the data was properly populated into the
correct table.
