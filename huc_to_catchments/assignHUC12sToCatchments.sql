-- Postgres script for assigning HUC12 values to catchments based on spatial relationships.

-- Conditions:
-- 1. Intermediate: Flowlines have a single HUC12 intersection. They get paired to that HUC12.
-- 2. Headwaters:   Flowlines that intersect multiple HUC12s and do not exist in the NextDownID field.
--                  These flowlines get assigned to the HUC that their downstream flowlines (NextDownID)
--                  are assigned to. 
-- 3. Mouths:       Flowlines that are at the mouth of a HUC12 and cross between 2 HUC 12s. These get 
--                  assigned based on the proportion of their length.

-- Test case
-- =========
--temp.trunc_hc = gis.truncated_flowlines
--temp.cat_hc = gis.catchments


-- Prep
-- ====

-- Drop previous iteration leftovers
drop schema temp cascade;
drop table cathuc12;

-- Keep temporary files separate
CREATE SCHEMA temp;


-- Add these restrictions on at the end of the script instead
-- Create the catchment/huc table
--CREATE TABLE data.cathuc12 (
--  featureid  bigint,
--             REFERENCES catchments (featureid)
--			   UNIQUE,
--  huc12      varchar(20)
--);


-- Create the catchment/huc table (add restrictions at end)
CREATE TABLE data.cathuc12 (
  featureid  bigint,
  huc12      varchar(20)
);


-- HUC 12 Pre-processing
-- =====================
-- Index relevant HUC 12s by hydrologic region to save processing effort
WITH select_hucs AS( 
  WITH usa_hucs AS (
    SELECT *
    FROM gis.wbdhu12
    WHERE huc12 NOT IN ('CANADA')
  )
  SELECT * 
  FROM usa_hucs
  WHERE
    CAST(huc12 AS numeric) < 070000000000
)
SELECT select_hucs.*, remove INTO temp.all_hu12
  FROM 
    select_hucs
	LEFT JOIN data.ocean_huc12s oh ON select_hucs.huc12=oh.huc12
;
  
-- Remove ocean HUC12s
SELECT * INTO temp.hu12
  FROM 
    temp.all_hu12 ah
  WHERE remove IS NULL; 

ALTER TABLE temp.hu12 ADD PRIMARY KEY (fid);
CREATE INDEX hu12_geom_gist ON temp.hu12 USING gist(geom);

DROP TABLE temp.all_hu12;


-- Add transformed geometry columns
-- ================================
-- Allows for preservation of geometry indexes in PostGIS functions requiring 
--     equal area projections. Also saves time from transforming every time.

-- HUC 12 
-- ------
ALTER TABLE temp.hu12 
  ADD COLUMN geom_2163 geometry(Geometry,2163);
UPDATE temp.hu12 
  SET geom_2163 = ST_Transform(geom, 2163)
  FROM spatial_ref_sys 
  WHERE ST_SRID(geom) = srid;
CREATE INDEX hu12_geom_2163_gist ON temp.hu12 USING gist(geom_2163);

-- Flowlines
-- ---------
ALTER TABLE gis.truncated_flowlines 
  ADD COLUMN geom_2163 geometry(Geometry,2163);
UPDATE gis.truncated_flowlines 
  SET geom_2163 = ST_Transform(geom, 2163)
  FROM spatial_ref_sys 
  WHERE ST_SRID(geom) = srid;
  
CREATE INDEX trunacted_flowlines_geom_2163_gist 
  ON gis.truncated_flowlines USING gist(geom_2163);

--SELECT Find_SRID('temp', 'hu12', 'geom_2163');
--SELECT Find_SRID('gis', 'truncated_flowlines', 'geom_2163');


-- ============================================================================
--                   Assign HUC 12s - Flowline Method
-- ============================================================================

-- Flowline Intersection Table
-- ===========================
-- Determine the number of HUC12 intersections for each flowline, including 
--   those that do not intersect a HUC12.

-- Flowline/huc intersections
-- --------------------------
SELECT t.featureid, t.nextdownid, h.huc12 
  INTO temp.intersect_lines
  FROM 
    gis.truncated_flowlines AS t
    INNER JOIN temp.hu12 AS h
    ON ST_Intersects(h.geom_2163, t.geom_2163);

CREATE UNIQUE INDEX intersections_featureid_huc12_idx 
  ON temp.intersect_lines (featureid, huc12);

-- Flowline/huc non-intersections
-- ------------------------------
SELECT tf.featureid, tf.nextdownid 
  INTO temp.non_intersect_lines
  FROM 
    gis.truncated_flowlines tf
    LEFT JOIN temp.intersect_lines il ON tf.featureid=il.featureid
  WHERE il.featureid IS NULL;

ALTER TABLE temp.non_intersect_lines 
  ADD COLUMN huc12 VARCHAR(20);
  
INSERT INTO temp.intersect_lines 
  SELECT featureid, nextdownid, huc12
  FROM temp.non_intersect_lines;


-- Match Count Table
-- =================
-- Table of the count of HUC12s of intersected by each featureid
SELECT featureid, nextdownid, COUNT(huc12) AS n_huc12 
  INTO temp.matches
  FROM temp.intersect_lines
  GROUP BY featureid, nextdownid
  ORDER BY n_huc12 DESC;

  
-- Identify flowline types
-- =======================  

-- Headwater flowlines
-- -------------------
-- ID lines that do not exist in the 'nextdownid' column
SELECT DISTINCT ON (tf1.featureid) tf1.featureid, tf1.nextdownid, tf2.featureid AS nextupid 
  INTO temp.headwaters
  FROM 
    truncated_flowlines tf1
    LEFT JOIN truncated_flowlines tf2 ON tf1.featureid = tf2.nextdownid
  ORDER BY tf1.featureid DESC;

ALTER TABLE temp.headwaters ADD COLUMN type_id VARCHAR(12);

UPDATE temp.headwaters 
  SET type_id = 'headwater'
  WHERE nextupid IS NULL
    AND nextdownid != -1;

-- Add type_id column by left joining headwater table
SELECT m.featureid, m.nextdownid, m.n_huc12, h.type_id INTO temp.match_count_lines
  FROM 
    temp.matches m
	LEFT JOIN temp.headwaters h ON m.featureid=h.featureid;

CREATE UNIQUE INDEX match_count_lines_featureid_n_huc12_idx ON temp.match_count_lines (featureid, n_huc12);
CREATE INDEX match_count_lines_nextdownid_idx ON temp.match_count_lines (nextdownid);
	
DROP TABLE temp.matches;
	
-- Intermediate flowlines
-- ----------------------
UPDATE temp.match_count_lines 
  SET type_id = 'intermediate'
  WHERE n_huc12 = 1 
    AND type_id IS NULL;
  
-- Mouth flowlines
-- ---------------
UPDATE temp.match_count_lines 
  SET type_id = 'mouth'
  WHERE n_huc12 > 1 
    AND type_id IS NULL;  

-- Isolated flowlines
-- ------------------
UPDATE temp.match_count_lines 
  SET type_id = 'isolated'
  WHERE n_huc12 = 0 
    AND type_id IS NULL; 
    
  
-- Intermediate Flowlines
-- ======================
-- If a flowline has only 1 intersection it gets assigned that HUC12
INSERT INTO cathuc12 (featureid, huc12) (
  SELECT il.featureid, huc12
    FROM 
      temp.intersect_lines il
	  LEFT JOIN temp.match_count_lines mcl ON il.featureid=mcl.featureid
  WHERE type_id LIKE 'intermediate'
); 


-- Mouth Flowlines
-- ===============
-- Flowlines that are at the mouth of a HUC12 and cross between 2 HUC 12s. 
--  These get assigned based on the proportion of their length.
INSERT INTO cathuc12 (featureid, huc12) (
  WITH t1 AS ( -- table of m featureids with intersection geometry
    WITH mo AS( --table of yet unassigned featureids with multiple intersections
      SELECT featureid, huc12
        FROM temp.intersect_lines
        WHERE featureid IN ( -- <-- make this a left join to run faster
          SELECT featureid
            FROM temp.match_count_lines
            WHERE type_id LIKE 'mouth'
	    )
    )
    -- Calculate intersection proportion of total length
	SELECT
      mo.featureid,
      mo.huc12,
      ST_Length(ST_Intersection(h.geom_2163, t.geom_2163))/ST_Length(t.geom_2163)
				AS fraction_length
    FROM 
      mo
	  LEFT JOIN temp.hu12 h ON mo.huc12=h.huc12 -- huc12 ids
      LEFT JOIN gis.truncated_flowlines t ON mo.featureid=t.featureid -- flowline ids
  )
  SELECT DISTINCT ON (featureid) featureid, huc12
    FROM   t1
    ORDER  BY featureid, fraction_length DESC
);


-- ============================================================================
--                   Assign HUC 12s - Catchment Method
-- ============================================================================
-- Some catchments do not have associated streams in the truncated_flowlines layer. 
--   These catchments, which have yet to be assigned a HUC 12 ID, are processed 
--   in this section.


-- Table Prep
-- ==========
-- Prepare the selected catchments as the huc layer in the previous section
SELECT * INTO temp.catchments_left
FROM gis.catchments
WHERE source -- Haven't already been processed
  LIKE 'Coastal Fill' 
OR featureid IN ( -- Flowlines do not intersect with HUC layer
  SELECT featureid
  FROM temp.match_count_lines
  WHERE n_huc12 = 0 AND type_id LIKE 'isolated'
  );

-- Add primary key + index
ALTER TABLE temp.catchments_left ADD PRIMARY KEY (gid);
CREATE INDEX catchments_left_geom_gist 
  ON temp.catchments_left USING gist(geom);

-- Add transformed geometry column
ALTER TABLE temp.catchments_left ADD COLUMN geom_2163 geometry(Geometry,2163);

UPDATE temp.catchments_left SET geom_2163 = ST_Transform(geom, 2163)
FROM spatial_ref_sys WHERE ST_SRID(geom) = srid;

CREATE INDEX catchments_left_geom_2163_gist 
  ON temp.catchments_left USING gist(geom_2163);


-- Catchment Intersection Tables
-- =============================
-- Create a table of all of the remaining catchment/huc intersections

-- Catchment/huc intersections
-- ---------------------------
SELECT c.featureid, h.huc12 
  INTO temp.intersect_cats
  FROM 
    temp.catchments_left AS c
    INNER JOIN temp.hu12 AS h
    ON ST_Intersects(h.geom_2163, c.geom_2163);

CREATE UNIQUE INDEX intersect_cats_featureid_huc12_idx ON temp.intersect_cats (featureid, huc12);

-- Catchment/huc non-intersections
-- -------------------------------
SELECT cl.featureid 
  INTO temp.non_intersect_cats
  FROM 
    temp.catchments_left cl
    LEFT JOIN temp.intersect_cats ic ON cl.featureid=ic.featureid
WHERE ic.featureid IS NULL;

ALTER TABLE temp.non_intersect_cats 
  ADD COLUMN huc12 VARCHAR(20);
  
INSERT INTO temp.intersect_cats 
  SELECT featureid, huc12 
  FROM temp.non_intersect_cats;


-- Match Count Table
-- =================
-- Create a table of the number of intersections by featureid
SELECT featureid, COUNT(huc12) AS n_huc12 
  INTO temp.match_count_cats
  FROM temp.intersect_cats
  GROUP BY featureid
  ORDER BY n_huc12 DESC;


-- Single Intersect Catchments
-- ===========================
-- Catchments that intersect a single HUC12 polygon get assigned that HUC12. 
INSERT INTO cathuc12 (featureid, huc12) (
  SELECT ic.featureid, huc12
  FROM 
    temp.intersect_cats ic
	LEFT JOIN temp.match_count_cats mcc ON ic.featureid=mcc.featureid
  WHERE n_huc12 = 1
);


-- Multi-Intersect Catchments
-- ==========================
-- Catchments that intersect multiple HUC12 polygons get assigned based on the
--   greatest area of intersection
INSERT INTO cathuc12 (featureid, huc12) (
  -- tg is a table of si featureids with intersection geometry
  WITH tg AS (
  -- si is table of selected intersections (not yet assigned and multiple intersections)
    WITH si AS(
      SELECT ic.featureid, huc12
      FROM 
        temp.intersect_cats ic
	    LEFT JOIN temp.match_count_cats mcc ON ic.featureid=mcc.featureid
      WHERE n_huc12 > 1
    )
    SELECT
      si.featureid,
      si.huc12,
      ST_Area(ST_Intersection(h.geom_2163, c.geom_2163))/ST_Area(c.geom_2163) as fraction_area
    FROM 
      si
      LEFT JOIN temp.hu12 h ON si.huc12=h.huc12 -- join huc12 columns for select intersections
      LEFT JOIN temp.catchments_left c ON si.featureid=c.featureid  -- join truncated flowline columns for select intersections
    )
  SELECT DISTINCT ON (featureid) featureid, huc12--, fraction_area
  FROM tg
  ORDER BY featureid, fraction_area DESC
);


-- Zero-Intersect Catchments
-- =========================
-- There are a few catchments that don't intersect any of the HUC12 polygons.

-- Index non-intersecting catchments for processing
SELECT cl.* INTO temp.free_cats
FROM 
  temp.catchments_left cl
  LEFT JOIN temp.match_count_cats mcc ON cl.featureid=mcc.featureid
where n_huc12 = 0;
CREATE INDEX free_cats_geom_2163_gist ON temp.free_cats USING gist(geom_2163);

-- Find the nearest HUC12s (within a reasonable distance of 5km) and select the nearest one.
INSERT INTO cathuc12 (featureid, huc12) (
  WITH x AS (
    SELECT featureid, huc12, st_distance(h.geom_2163, c.geom_2163) AS dist
    FROM temp.hu12 h, temp.free_cats c 
    WHERE st_dwithin(h.geom_2163, c.geom_2163, 5000)
  )
  SELECT DISTINCT ON (featureid) featureid, huc12
  FROM x
  GROUP BY featureid, huc12, dist
  ORDER BY featureid, dist
);


--=========================================================================================
--                                 HEADWATER ASSIGNMENT
--=========================================================================================

insert into cathuc12(featureid, huc12) (
  select mcl.featureid, ch.huc12 
  from 
    temp.match_count_lines mcl
    LEFT JOIN cathuc12 ch ON mcl.nextdownid=ch.featureid
  where type_id LIKE 'headwater'
);


-- END OF ASSIGNMENT SCRIPT



--=========================================================================================
--                                   QAQC
--=========================================================================================

-- Export cat/huc relationship list
psql -d sheds_new -c"COPY cathuc12 TO STDOUT WITH CSV HEADER" > /home/kyle/cathuc12_06022016.csv


-- Export headwaters with multiple huc intersections in order to 
-- 	get a better look at where some issues might exist (manual check)
select * into temp.hw_issues
  from temp.match_count_lines 
  where type_id like 'headwater' 
    and n_huc12 > 1;

	
select * 
  from temp.match_count_lines 
  where featureid = 201117643;
 
select * 
  from temp.hw_issues 
  where featureid = 201117643; 
 
select * 
  from temp.hw_issues 
  where featureid = 201117643;  
 
select distinct featureid into temp.test
  from temp.hw_issues;

 
  
  where type_id like 'headwater' 
    and n_huc12 > 1;	
	
	
	
	
	
-- Export 
psql -d sheds_new -c"COPY temp.hw_issues TO STDOUT WITH CSV HEADER" > /home/kyle/hw_issues_06022016.csv





-- clean out short steps

drop table temp.catchments_left, temp.free_cats, temp.headwaters, temp.match_count_cats, temp.match_count_lines, temp.non_intersect_cats, cathuc12;


-- ===========================================================
--                          ERRORS
-- ===========================================================
select * 
from cathuc12
where featureid = 201443949; -- huc12 = 0

select * 
from cathuc12
where featureid = 2011177312; --huc12 is null



select * 
from cathuc12
where huc12 is null;






select *
from 
  cathuc12 ch
  LEFT JOIN temp.match_count_lines mcl ON mcl.featureid=ch.featureid
where huc12 is null;

headwater and nextdownid = -1




  CAST(huc12 AS numeric) < 010000000000
;














psql -d sheds_new -c"COPY cathuc12 TO '/home/kyle/cathuc12.csv' DELIMITER ',' CSV HEADER;










-- Headwater Flowlines
-- ===================
-- Flowlines that intersect multiple HUC12s and do not exist in the NextDownID 
--  field are considered headwaters. These flowlines get assigned to the HUC 
--  that their downstream flowline (NextDownID) is assigned to.

EXPLAIN ANALYZE INSERT INTO cathuc12 (featureid, huc12) (
  SELECT featureid, huc12
  FROM temp.intersect_lines
  WHERE featureid NOT IN ( -- Haven't already been processed
    SELECT featureid 
	FROM cathuc12)
  AND featureid IN ( -- Doesn't have multiple HUC assignments
    SELECT featureid
    FROM temp.match_count_lines
    WHERE n_huc12 = 1) -- this should be "> 1"?
  AND featureid NOT IN ( -- Qualify as headwater stream
    SELECT nextdownid 
    FROM gis.truncated_flowlines
  )
);




--INSERT INTO cathuc12 (featureid, huc12) (


SELECT featureid, huc12
FROM temp.intersect_lines
WHERE featureid IN () -- Doesn't have multiple HUC assignments

EXPLAIN ANALYZE SELECT featureid
FROM temp.match_count_lines
WHERE n_huc12 > 1 -- this should be "> 1"?
AND featureid NOT IN ( -- Qualify as headwater stream
  SELECT nextdownid 
  FROM gis.truncated_flowlines
);


SELECT featureid into temp.headwaters
FROM temp.match_count_lines
WHERE n_huc12 > 1
AND featureid NOT IN ( -- Qualify as headwater stream
  SELECT nextdownid 
  FROM gis.truncated_flowlines
);


select * into temp.jointest
from temp.headwaters;



SELECT hw.featureid, nextdownid into temp.test
  from temp.headwaters hw
  LEFT JOIN gis.truncated_flowlines tf ON hw.featureid=tf.featureid


-- Assign headwater streams to their nextdownid's HUC12
with y as (
  SELECT hw.featureid, nextdownid
  from temp.headwaters hw
  LEFT JOIN gis.truncated_flowlines tf ON hw.featureid=tf.featureid
)
select y.featureid, y.nextdownid, c.huc12 into temp.test
from y
left join cathuc12 c ON y.nextdownid=c.featureid;

  
select nextdownid


LEFT JOIN temp.hu12 h ON ch.huc12=h.huc12



















