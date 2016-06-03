#!/bin/bash
# imports the manually created list of oceanic huc12s
# csv file must have 1 column: huc12

# usage: $ ./upload_ocean_huc12s.sh <db name> <path to huc12 list CSV file>
# example: $ ./upload_ocean_huc12s.sh sheds_new /home/kyle/huccat/tables/ocean_huc12s.csv


set -eu
set -o pipefail

DB=$1
FILE=$2




psql -d $DB -c  "CREATE TABLE data.ocean_huc12s (
                   huc12      varchar(20)
                 );"


psql -v ON_ERROR_STOP=1 -1 -d $DB -c "\COPY data.ocean_huc12s FROM $FILE DELIMITER ',' CSV HEADER NULL AS 'NA';" || { echo "Failed to import covariates csv file";}


psql -d $DB -c  "UPDATE data.ocean_huc12s
                   SET huc12 = '0' || huc12;

                ALTER TABLE data.ocean_huc12s 
                   ADD COLUMN remove int;

                UPDATE data.ocean_huc12s 
                   SET remove = 1;"
				   