
-- Setup the table for testing
-- ===========================

createdb huccat
psql huccat -c "CREATE EXTENSION postgis;"

pg_dump -t wbdhu12 sheds_new | psql huccat


CREATE SCHEMA gis



-- Flowlines
-- ---------
CREATE TABLE truncated_flowlines (
    source            varchar(80),
    featureid         bigint, 
    nextdownid        bigint, 
    shape_leng        real,
	lengthkm          real
);
ALTER TABLE truncated_flowlines ADD COLUMN geom geometry(POLYGON,4326);

-- Catchments
-- ----------
CREATE TABLE catchments (
    gid            int, 
    objectid       int, 
    shape_leng     numeric, 
    shape_area     numeric, 
	hydroid        int,
	gridid         int,
	nextdownid     bigint,
	riverorder     int,
	featureid      bigint,
	areasqkm       numeric,
	source         varchar(20)
);
ALTER TABLE catchments ADD COLUMN geom geometry(POLYGON,4326);

CREATE TABLE catchment_huc12 (
    featureid       bigint,
    huc12           varchar(20)
);






wbdhu12