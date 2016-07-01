#!/bin/bash
# imports the manually created list of oceanic huc12s
# csv file must have 1 column: huc12

# usage: $ ./upload_ocean_huc12s.sh <db name> <path to huc12 list CSV file>
# example: $ ./upload_ocean_huc12s.sh sheds /home/kyle/hydrography_crosswalk/huc_to_catchments/tables/ocean_huc12s.csv


set -eu
set -o pipefail

DB=$1
FILE=$2

# Create the table + update permissions
psql -d $DB -c  "CREATE TABLE data.ocean_huc12s (
                   huc12      varchar(20)
                 );


                GRANT ALL PRIVILEGES ON data.ocean_huc12s TO jeff, sheds_admin;
                GRANT SELECT ON data.ocean_huc12s TO sheds_read;
                GRANT UPDATE, INSERT, DELETE ON data.ocean_huc12s TO sheds_write;"

# Copy the table from the CSV
psql -v ON_ERROR_STOP=1 -1 -d $DB -c "\COPY data.ocean_huc12s FROM $FILE DELIMITER ',' CSV HEADER NULL AS 'NA';" || { echo "Failed to import covariates csv file";}

# Add the leading zero to the huc12 (truncated in CSV) and add a column for removal in processing
psql -d $DB -c  "UPDATE data.ocean_huc12s
                   SET huc12 = '0' || huc12;

                ALTER TABLE data.ocean_huc12s 
                   ADD COLUMN remove int;

                UPDATE data.ocean_huc12s 
                   SET remove = 1;"
				   