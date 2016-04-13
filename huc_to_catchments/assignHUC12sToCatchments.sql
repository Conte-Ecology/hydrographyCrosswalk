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
--select * from information_schema.table_constraints where table_name='cathuc12';


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




-- ============================================================================
--                   Assign HUC 12s - Flowline Method
-- ============================================================================


-- Intersection Tables
-- ===================
-- Determine the number of HUC12 intersections for each flowline.

-- Table of all flowline/huc intersections
SELECT t.featureid, w.huc12 INTO temp.intersect_lines
FROM gis.truncated_flowlines AS t
INNER JOIN temp.hu12 AS w
ON ST_Intersects(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163));

--create unique index intersections_featureid_huc12_idx on temp.intersect_lines (featureid, huc12);
--drop index temp.intersections_featureid_huc12_idx;


-- Table of the number of HUC12s of intersected by each featureid
SELECT featureid, COUNT(huc12) AS n_huc12 INTO temp.match_count_lines
FROM temp.intersect_lines
GROUP BY featureid
ORDER BY n_huc12 DESC;




-- Intermediate Flowlines
-- ======================
-- If a flowline has only 1 intersection it gets assigned that HUC12
INSERT INTO cathuc12 (featureid, huc12) (
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
      ST_Length(ST_Intersection(ST_Transform(h.geom, 2163), 
	                            ST_Transform(t.geom, 2163)) 
	            ) / ST_Length(ST_Transform(t.geom, 2163))
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

-- Intersection Tables
-- ===================
-- Create a table of all of the remaining catchment/huc intersections
with c as (
  select *
  from gis.catchments
  where featureid not in ( -- Haven't already been processed
    select featureid 
    from cathuc12)
)
select c.featureid, h.huc12 into temp.intersect_cats
from c
inner join temp.hu12 as h
on ST_Intersects(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163));


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
      ST_Area(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) / ST_Area(ST_Transform(t.geom, 2163)) as fraction_area
    from 
      si
      left join temp.hu12 w on si.huc12=w.huc12 -- join huc12 columns for select intersections
      left join gis.catchments t on si.featureid=t.featureid  -- join truncated flowline columns for select intersections
    )
  SELECT DISTINCT ON (featureid) featureid, huc12--, fraction_area
  FROM   tg
  ORDER  BY featureid, fraction_area DESC
);



-- Zero-Intersect Catchments
-- =========================
-- Catchments that don't intersect any of the HUC12 polygons. There are few of these.

select src.* into temp.free_cats
from gis.catchments src
where src.featureid not in (
  select featureid 
  from cathuc12
);


insert into cathuc12 (featureid, huc12) (
  with x as (
    select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
    from temp.hu12 h, temp.free_cats c   
    where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 5000)
  )
  select DISTINCT ON (featureid) featureid, huc12
  from x
  group by featureid, huc12, dist
  order by featureid, dist
);


--=========================================================================================
--                       END CATCHMENT ASSIGNMENT
--=========================================================================================