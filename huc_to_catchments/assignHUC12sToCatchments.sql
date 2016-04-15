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

-- Create the catchment/huc table
CREATE TABLE data.cathuc12 (
  featureid  bigint
             REFERENCES catchments (featureid)
			 UNIQUE,
  huc12      varchar(20)
);


-- Index relevant HUC 12s by hydrologic region to save processing effort
with usa_hucs as (
  select *
  from gis.wbdhu12
  where huc12 NOT IN ('CANADA')
)
select * INTO temp.hu12
from usa_hucs
where
  CAST(huc12 AS numeric) < 070000000000
;

-- Add primary key
ALTER TABLE temp.hu12 ADD PRIMARY KEY (fid);

-- Add geometry index
CREATE INDEX hu12_geom_gist ON temp.hu12 USING gist(geom);





-- Add transformed geometry columns
-- ================================

-- HUC 12 
-- ------
ALTER TABLE temp.hu12 ADD COLUMN geom_2163 geometry(Geometry,2163);

UPDATE temp.hu12 SET geom_2163 = ST_Transform(geom, 2163)
FROM spatial_ref_sys WHERE ST_SRID(geom) = srid;

CREATE INDEX hu12_geom_2163_gist ON temp.hu12 USING gist(geom_2163);


-- Flowlines
-- ---------
ALTER TABLE gis.truncated_flowlines ADD COLUMN geom_2163 geometry(Geometry,2163);

UPDATE gis.truncated_flowlines SET geom_2163 = ST_Transform(geom, 2163)
FROM spatial_ref_sys WHERE ST_SRID(geom) = srid;

CREATE INDEX trunacted_flowlines_geom_2163_gist ON gis.truncated_flowlines USING gist(geom_2163);


--SELECT Find_SRID('temp', 'hu12', 'geom_2163');
--SELECT Find_SRID('gis', 'truncated_flowlines', 'geom_2163');


-- ============================================================================
--                   Assign HUC 12s - Flowline Method
-- ============================================================================

-- Intersection Tables
-- ===================
-- Determine the number of HUC12 intersections for each flowline.

-- Table of all flowline/huc intersections
SELECT t.featureid, h.huc12 INTO temp.intersect_lines
FROM gis.truncated_flowlines AS t
INNER JOIN temp.hu12 AS h
ON ST_Intersects(h.geom_2163, t.geom_2163);
-- This took about 16 hours to run for the entire range (57384676.578ms)



--create unique index intersections_featureid_huc12_idx on temp.intersect_lines (featureid, huc12);
--drop index temp.intersections_featureid_huc12_idx;


-- Table of the number of HUC12s of intersected by each featureid
EXPLAIN SELECT featureid, COUNT(huc12) AS n_huc12 INTO temp.match_count_lines
FROM temp.intersect_lines
GROUP BY featureid
ORDER BY n_huc12 DESC;
-- 98216.736ms



-- Intermediate Flowlines
-- ======================
-- If a flowline has only 1 intersection it gets assigned that HUC12
EXPLAIN INSERT INTO cathuc12 (featureid, huc12) (
  SELECT featureid, huc12
  FROM temp.intersect_lines
  WHERE featureid in (
    SELECT featureid
    FROM temp.match_count_lines
    WHERE n_huc12 = 1
  )
);


-- Headwater Flowlines
-- ===================
-- Flowlines that intersect multiple HUC12s and do not exist in the NextDownID 
--  field are considered headwaters. These flowlines get assigned to the HUC 
--  that their downstream flowline (NextDownID) is assigned to.

INSERT INTO cathuc12 (featureid, huc12) (
  SELECT featureid, huc12
  FROM temp.intersect_lines
  WHERE featureid NOT IN ( -- Haven't already been processed
    SELECT featureid 
	FROM cathuc12)
  AND featureid IN ( -- Doesn't have multiple HUC assignments
    SELECT featureid
    FROM temp.match_count_lines
    WHERE n_huc12 = 1)
  AND featureid NOT IN ( -- Qualify as headwater stream
    SELECT nextdownid 
    FROM gis.truncated_flowlines
  )
);


-- Mouth Flowlines
-- ===============
-- Flowlines that are at the mouth of a HUC12 and cross between 2 HUC 12s. 
--  These get assigned based on the proportion of their length.

INSERT INTO cathuc12 (featureid, huc12) (
  WITH t1 AS ( -- table of ch featureids with intersection geometry
    WITH ch AS( --table of yet unassigned featureids with multiple intersections
      SELECT *
      FROM temp.intersect_lines
      WHERE featureid NOT IN (
        SELECT featureid 
        FROM cathuc12)
	  AND featureid IN (
        SELECT featureid
        FROM temp.match_count_lines
        WHERE n_huc12 > 1)
    )
    -- Calculate intersection proportion of total length
	SELECT
      ch.featureid,
      ch.huc12,
      ST_Length(ST_Intersection(h.geom_2163, t.geom_2163))/ST_Length(t.geom_2163)
				AS fraction_length
    FROM 
      ch
	  LEFT JOIN temp.hu12 h ON ch.huc12=h.huc12 -- huc12 ids
      LEFT JOIN gis.truncated_flowlines t ON ch.featureid=t.featureid -- flowline ids
  )
  SELECT DISTINCT ON (featureid) featureid, huc12
  FROM   t1
  ORDER  BY featureid, fraction_length DESC
);


-- ============================================================================
--                   Assign HUC 12s - Catchment Method
-- ============================================================================
-- Some catchments do not have associated sterams in the truncated_flowlines layer. 
--   These catchments, which have yet to be assigned a HUC 12 ID, are processed 
--   in this section.



-- Table Prep
-- ==========

-- Prepare the selected catchments as the huc layer in the previous section
select * into temp.catchments_left
from gis.catchments
where featureid not in ( -- Haven't already been processed
  select featureid 
  from cathuc12)

-- Add primary key + index
ALTER TABLE temp.catchments_left ADD PRIMARY KEY (gid);
CREATE INDEX catchments_left_geom_gist ON temp.catchments_left USING gist(geom);

-- Add transformed geometry column
ALTER TABLE temp.catchments_left ADD COLUMN geom_2163 geometry(Geometry,2163);

UPDATE temp.catchments_left SET geom_2163 = ST_Transform(geom, 2163)
FROM spatial_ref_sys WHERE ST_SRID(geom) = srid;

CREATE INDEX catchments_left_geom_2163_gist ON temp.catchments_left USING gist(geom_2163);



-- Intersection Tables
-- ===================

-- Create a table of all of the remaining catchment/huc intersections
select c.featureid, h.huc12 into temp.intersect_cats
from catchments_left as c
inner join temp.hu12 as h
on ST_Intersects(h.geom_2163, c.geom_2163);


-- Create a table of the number of intersections by featureid
select featureid, count(huc12) as n_huc12 into temp.match_count_cats
from temp.intersect_cats
group by featureid
order by n_huc12 desc;


-- Single Intersect Catchments
-- ===========================
-- Catchments that intersect a single HUC12 polygon get assigned that HUC12. 
insert into cathuc12 (featureid, huc12) (
  select featureid, huc12
  from temp.intersect_cats
  where featureid in (
    select featureid 
    from temp.match_count_cats
    where n_huc12 = 1
  )
);



-- Multi-Intersect Catchments
-- ==========================
-- Catchments that intersect multiple HUC12 polygons get assigned based on the
--   greatest area of intersection

insert into cathuc12 (featureid, huc12) (
  -- tg is a table of si featureids with intersection geometry
  with tg as (
  -- si is table of selected intersections (not yet assigned and multiple intersections)
    with si as(
      select *
      from temp.intersect_cats
      where featureid not in ( 
        select featureid 
        from cathuc12)
	  and featureid in (
        select featureid
        from temp.match_count_cats
        where n_huc12 > 1)
    )
    select
      si.featureid,
      si.huc12,
      ST_Area(ST_Intersection(h.geom_2163, c.geom_2163))/ST_Area(t.geom_2163) as fraction_area
    from 
      si
      left join temp.hu12 h on si.huc12=h.huc12 -- join huc12 columns for select intersections
      left join temp.catchments_left c on si.featureid=c.featureid  -- join truncated flowline columns for select intersections
    )
  SELECT DISTINCT ON (featureid) featureid, huc12--, fraction_area
  FROM   tg
  ORDER  BY featureid, fraction_area DESC
);



-- Zero-Intersect Catchments
-- =========================
-- Catchments that don't intersect any of the HUC12 polygons. There are few of these.

select c.* into temp.free_cats
from temp.catchments_left c
where c.featureid not in (
  select featureid 
  from cathuc12
);

CREATE INDEX free_cats_geom_2163_gist ON temp.free_cats USING gist(geom_2163);



insert into cathuc12 (featureid, huc12) (
  with x as (
    select featureid, huc12, st_distance(h.geom_2163, c.geom_2163) as dist
    from temp.hu12 h, temp.free_cats c 
    where st_dwithin(h.geom_2163, c.geom_2163, 5000)
  )
  select DISTINCT ON (featureid) featureid, huc12
  from x
  group by featureid, huc12, dist
  order by featureid, dist
);


--=========================================================================================
--                       END CATCHMENT ASSIGNMENT
--=========================================================================================