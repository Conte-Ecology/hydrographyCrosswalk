#!/bin/bash
# imports the list of manually assigned huc12-catchment relationships
# csv file must have 2 columns: featureid and huc12

# usage: $ ./upload_manual_huc12s.sh <db name> <path to huc12 list CSV file>
# example: $ ./upload_manual_huc12s.sh sheds /home/kyle/hydrography_crosswalk/huc_to_catchments/tables/manual_huc12s.csv


set -eu
set -o pipefail

DB=$1
FILE=$2

# Create the table of manually edited huc12 assignemnts + update permissions
psql -d $DB -c  "CREATE TABLE data.manual_huc12s (
                   featureid  bigint, 
                   huc12  varchar(20)
                 );

                GRANT ALL PRIVILEGES ON data.manual_huc12s TO jeff, sheds_admin;
                GRANT SELECT ON data.manual_huc12s TO sheds_read;
                GRANT UPDATE, INSERT, DELETE ON data.manual_huc12s TO sheds_write;"

# Populate the table from the CSV
psql -v ON_ERROR_STOP=1 -1 -d $DB -c "\COPY data.manual_huc12s FROM $FILE DELIMITER ',' CSV HEADER NULL AS 'NA';" || { echo "Failed to import covariates csv file";}

# Add the leading zero to the huc12s
psql -d $DB -c  "UPDATE data.manual_huc12s
                   SET huc12 = '0' || huc12;"
				   