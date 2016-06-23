#!/bin/bash
# imports the manually created list of oceanic huc12s
# csv file must have 1 column: huc12

# usage: $ ./upload_manual_huc12s.sh <db name> <path to huc12 list CSV file>
# example: $ ./upload_manual_huc12s.sh sheds_new /home/kyle/hydrography_crosswalk/huc_to_catchments/tables/manual_huc12s.csv


set -eu
set -o pipefail

DB=$1
FILE=$2

# Create the table of manually edited huc12 assignemnts
psql -d $DB -c  "CREATE TABLE data.manual_huc12s (
                   featureid  bigint, 
                   huc12  varchar(20)
                 );"

# Populate the table from the CSV
psql -v ON_ERROR_STOP=1 -1 -d $DB -c "\COPY data.manual_huc12s FROM $FILE DELIMITER ',' CSV HEADER NULL AS 'NA';" || { echo "Failed to import covariates csv file";}

# Add the leading zero to the huc12s
psql -d $DB -c  "UPDATE data.manual_huc12s
                   SET huc12 = '0' || huc12;"
				   