
-- Conditions:
-- 1. Intermediate: Flowlines have a single HUC12 intersection. They get paired to that HUC12.
-- 2. Headwaters:   Flowlines that intersect multiple HUC12s and do not exist in the NextDownID field.
--                  These flowlines get assigned to the HUC that their downstream flowlines (NextDownID)
--                  are assigned to. 
-- 3. Mouths:       Flowlines that are at the mouth of a HUC12 and cross between 2 HUC 12s. These get 
--                  assigned based on the proportion of their length.



drop schema temp cascade;
drop table cathuc12;

--CREATE SCHEMA temp;





--SELECT ST_Transform(c.geom, 2163)
--from catchments

select * INTO temp.hu12
from gis.wbdhu12
where
  huc12 in ('010900010205', '010900010302', '010900010301');




-- Index relevant HUC 12s by hydrologic region to save processing time
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


-- Transform  layers once before processing




  


-- Test case
-- =========
--temp.trunc_hc = gis.truncated_flowlines
--temp.cat_hc = gis.catchments


-- Prep
-- ====
-- Create the catchment/huc table
CREATE TABLE data.cathuc12 (
  featureid  bigint
             REFERENCES catchments (featureid)
			 UNIQUE,
  huc12      varchar(20)
);
--select * from information_schema.table_constraints where table_name='cathuc12';



-- TO DELETE:
--select w.* into temp.hu12 from wbdhu12 w where w.huc12 = '010900010901' OR  w.huc12 = '010900010902';




-- ============================================================================
--                   Assign HUC 12s - Flowline Method
-- ============================================================================


-- Intersection Tables
-- ===================
-- Determine the number of HUC12 intersections for each flowline.

-- Table of all flowline/huc intersections
SELECT t.featureid, w.huc12 INTO temp.intersect_lines
FROM temp.trunc_hc AS t
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
      LEFT JOIN temp.trunc_hc t ON ch.featureid=t.featureid -- flowline ids
  )
  SELECT DISTINCT ON (featureid) featureid, huc12
  FROM   t1
  ORDER  BY featureid, fraction_length DESC
);






-- ============================================================================
--                   Assign HUC 12s - Catchment Method
-- ============================================================================

-- Process the catchment that haven't been assigned yet


-- Create a table of all of the remaining catchment/huc intersections
with c as (
  select *
  from temp.cat_hc
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
      left join temp.cat_hc t on si.featureid=t.featureid  -- join truncated flowline columns for select intersections
    )
  SELECT DISTINCT ON (featureid) featureid, huc12--, fraction_area
  FROM   tg
  ORDER  BY featureid, fraction_area DESC
);



--=========================================================================================

-- Catchments with no intersections

select src.* into temp.free_cats
from temp.cat_hc src
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














select * into shorecats
from catchments 
where 
featureid in (201478880, 201489589, 201493020, 2011176971, 2011177095);





-- DEFINE TYPE:
CREATE TYPE nearest_huc_type as 
(featureid bigint, 
huc12 varchar(12));


-- >> FOR LOOP... in progress
CREATE OR REPLACE FUNCTION nearest_huc() --RETURNS TABLE(featureid bigint, huc12 varchar) AS 
--$$
RETURNS SETOF nearest_huc_type AS $$
DECLARE 
  stmt     TEXT;
  test nearest_huc_type; --TABLE(featureid bigint, huc12 varchar);
  num integer;
  i integer;
BEGIN
  stmt := ' with x as (
             select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
             from wbdhu12 h, shorecats c   
             where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 10^$1)
        )
        select DISTINCT ON (featureid) featureid, huc12
        from x
        group by featureid, huc12, dist
        order by featureid, dist'; 	
  
  FOR i IN 1..3 LOOP 
    EXECUTE stmt INTO test USING i; -- $1
	select count(*) into num from test;
	IF num = 5 THEN RETURN QUERY select * from test; END IF;
  END LOOP;
  --RETURN NEXT test;
END
$$ LANGUAGE 'plpgsql' STABLE;



select * from nearest_huc();
select nearest_huc() as f(featureid bigint, huc12 varchar(12));










-- >> WHILE LOOP NOT WORKING
CREATE OR REPLACE FUNCTION nearest_huc()
--$$
RETURNS SETOF nearest_huc_type AS $$
DECLARE 
  stmt     TEXT;
  test nearest_huc_type; --TABLE(featureid bigint, huc12 varchar);
  num integer;
  i integer;
BEGIN
  stmt := ' with x as (
             select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
             from wbdhu12 h, shorecats c   
             where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 10^$1)
        )
        select DISTINCT ON (featureid) featureid, huc12
        from x
        group by featureid, huc12, dist
        order by featureid, dist'; 	
  i := 1;	
  WHILE (num < 5) LOOP 
    EXECUTE stmt INTO test USING i; -- $1
	select count(*) into num from test;
	i := i + 1;
	--RETURN NEXT i;
  END LOOP;
  RETURN NEXT test;
END
$$ LANGUAGE 'plpgsql' STABLE;


select * from nearest_huc();

select nearest_huc() as f(featureid bigint, huc12 varchar(12));







CREATE TYPE nearest_huc_type as 
(featureid bigint, 
huc12 varchar);



CREATE OR REPLACE FUNCTION nearest_huc() --RETURNS TABLE(featureid bigint, huc12 varchar) AS 
--$$
RETURNS SETOF nearest_huc_type AS $$
DECLARE 
  stmt     TEXT;
  test nearest_huc_type%rowtype; --TABLE(featureid bigint, huc12 varchar); 
  num integer;
  --i integer;
BEGIN
  stmt := ' with x as (
             select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
             from temp.hu12 h, shorecats c   
             where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 100000)
        )
        select DISTINCT ON (featureid) featureid, huc12
        from x
        group by featureid, huc12, dist
        order by featureid, dist';
  EXECUTE stmt into test;
  select count(*) into num from test;
  --IF num > 0 THEN RETURN NEXT test; END IF;
  --SELECT CASE WHEN EXISTS (SELECT * FROM foo LIMIT 1) THEN RETURN NEXT test;
  --RETURN NEXT;
  if num = 5 then RETURN QUERY EXECUTE stmt else return query execute stmt limit 1; end if;
  RETURN QUERY EXECUTE stmt;
END
$$ LANGUAGE 'plpgsql' STABLE;

SELECT * FROM nearest_huc();



FOR i IN 1..3 LOOP 
  EXECUTE stmt INTO test USING i;
  select count(*) into num from test;
  IF num = 5 THEN RETURN QUERY select * from test; END IF;
END LOOP;










 with x as (
             select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
             from temp.hu12 h, shorecats c   
             where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 100000)
        )
        select DISTINCT ON (featureid) featureid, huc12
        from x
        group by featureid, huc12, dist
        order by featureid, dist;







CREATE table foo 
(featureid bigint, 
huc12 varchar(12));

SELECT CASE WHEN EXISTS (SELECT * FROM foo LIMIT 1) THEN 1
         ELSE 0 
END




EXECUTE stmt INTO test USING i; -- $1
	select count(*) into num from test;
    --IF num = 5 THEN RETURN NEXT test; END IF;
	IF num = 5 THEN RETURN QUERY select featureid, huc12 from test; END IF;



wbdhu12 h

with x as (
  select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
  from wbdhu12 h, shorecats c   
  where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 1000)
)
select DISTINCT ON (featureid) featureid, huc12, dist
from x
group by featureid, huc12, dist
order by featureid, dist;




-- Test that works
-- ===============
with x as (
  select featureid, huc12, st_distance(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163)) as dist
  from temp.hu12 h, shorecats c   
  where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 100000)
)
select DISTINCT ON (featureid) featureid, huc12, dist
from x
group by featureid, huc12, dist
order by featureid, dist;






























--CREATE OR REPLACE FUNCTION nearest_neighb() RETURNS VARCHAR AS 
CREATE OR REPLACE FUNCTION nearest_huc() RETURNS VARCHAR AS 
$$
DECLARE 
  sql     TEXT;
  result  varchar;
BEGIN
  sql := ' select huc12 
           from temp.hu12 h, shorecats c   
           where st_dwithin(h.geom, c.geom, 10^$1)
           order by st_distance(h.geom, c.geom)
           limit 1';
  FOR i IN 0..5 LOOP  
    EXECUTE 
	  'select huc12 
      from temp.hu12 h, shorecats c   
      where st_dwithin(h.geom, c.geom, 10^$1)
      order by st_distance(h.geom, c.geom)
      limit 1'
	INTO result USING i; -- $1
    IF result IS NOT NULL THEN RETURN result; END IF;
  END LOOP;
  RETURN NULL;
END
$$ LANGUAGE 'plpgsql' STABLE;

SELECT * FROM nearest_huc();










CREATE OR REPLACE FUNCTION 
  nn(nearTo                   geometry
   , initialDistance          REAL
   , distanceMultiplier       REAL 
   , maxPower                 INTEGER
   , nearThings               TEXT
   , nearThingsIdField        TEXT
   , nearThingsGeometryField  TEXT)
RETURNS INTEGER AS $$
DECLARE 
  sql     TEXT;
  result  INTEGER;
BEGIN
  sql := ' select ' || QUOTE_IDENT(nearThingsIdField) 
      || ' from '   || QUOTE_IDENT(nearThings)
      || ' where st_dwithin($1, ' 
      ||   QUOTE_IDENT(nearThingsGeometryField) || ', $2 * ($3 ^ $4))'
      || ' order by st_distance($1, ' || QUOTE_IDENT(nearThingsGeometryField) || ')'
      || ' limit 1';
  FOR i IN 0..maxPower LOOP
    EXECUTE sql INTO result USING nearTo              -- $1
                                , initialDistance     -- $2
                                , distanceMultiplier  -- $3
                                , i;                  -- $4
    IF result IS NOT NULL THEN RETURN result; END IF;
  END LOOP;
  RETURN NULL;
END
$$ LANGUAGE 'plpgsql' STABLE;
















select huc12
from temp.hu12 h, temp.leftcats c
where st_dwithin(ST_Transform(h.geom, 2163), ST_Transform(c.geom, 2163), 10000);
   
   
   
select huc12 ' || QUOTE_IDENT(nearThingsIdField) 
      || ' from temp.hu12 h, temp.leftcats c'   || QUOTE_IDENT(nearThings)
      || ' where st_dwithin(h.geom, c.geom, 1000)' 
      ||   QUOTE_IDENT(nearThingsGeometryField) || ', $2 * ($3 ^ $4))'

   
   
-- Cleanup
-- =======   


-- Drop interim tables
DROP TABLE temp.intersect_lines, temp.match_count_lines, temp.intersect_cats, temp.match_count_cats;




------------------------------------------------------------------------------------------------------------------------------------------------------------








-- Original Mouth Flowlines script:
-- ================================
with x as(
  -- featureids that haven't already been assigned
  with ch as(
    select *
    from temp.intersections
    where featureid not in ( 
      select featureid 
      from cathuc12)
  )
  
  select
    ch.featureid,
    ch.huc12,
    ST_Length(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) as intersect_length,
    ST_Length(ST_Transform(t.geom, 2163)) as flowline_length
  from 
   ch
   left join temp.hu12 w on ch.huc12=w.huc12
   left join temp.trunc_hc t on ch.featureid=t.featureid
)select featureid, huc12, intersect_length/flowline_length as fraction_length into fractions
from x;


-- Second Mouth Flowlines script:
-- ==============================
insert into cathuc12 (featureid, huc12) (
  
  -- t1 is table of ch featureids with intersection geometry
  with t1 as (
  -- ch is table of selected intersections (not yet assigned and multiple intersections)
    with ch as(
      select *
      from temp.intersections
      where featureid not in ( 
        select featureid 
        from cathuc12)
	  and featureid in (
        select featureid
        from temp.match_count_lines
        where n_huc12 > 1)
    )
    select
      ch.featureid,
      ch.huc12,
      ST_Length(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) / ST_Length(ST_Transform(t.geom, 2163)) as fraction_length
    from 
      ch
      left join temp.hu12 w on ch.huc12=w.huc12 -- join huc12 columns for select intersections
      left join temp.trunc_hc t on ch.featureid=t.featureid  -- join truncated flowline columns for select intersections
    )
  select 
    t1.featureid,
    t1.huc12
  from t1
  where t1.fraction_length = (
    select max(t2.fraction_length) 
    from fractions t2
    where t2.featureid = t1.featureid
  )
);

























 as intersect_length




temp.trunchc

select testtrunc* 
from truncated.flowlines
where featureid not in (select featureid from cathuc12)



select ST_Length(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) as intersect_length
from wbdhu12 w, temp.trunchc t;


with x as (
  select
    ch.featureid,
    ch.huc12,
    ST_Length(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) as intersect_length,
    ST_Length(ST_Transform(t.geom, 2163)) as flowline_length
  from catchment_huc12 ch
  left join wbdhu12 w on ch.huc12=w.huc12
  left join temp.trunchc t on ch.featureid=t.featureid
)
select featureid, huc12, intersect_area, catchment_area, intersect_area/catchment_area as fraction_area
from x
order by fraction_area


'POINT(0 0)'::geometry, 

SELECT ST_AsText(ST_Intersection('LINESTRING ( 2 0, 0 2 )'::geometry, 'POLYGON (( 1 1, 2 1, 2 2, 1 2 ))'::geometry));


SELECT * FROM 
 ST_Intersects(wbdhu12, 

  ST_Intersects(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))
 
 
 

with x as (
  select
    ch.featureid,
    ch.huc12,
    ST_Length(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))) as intersect_length,
    ST_Length(ST_Transform(t.geom, 2163)) as flowline_length
  from catchment_huc12 ch
  left join wbdhu12 w on ch.huc12=w.huc12
  left join truncated_flowlines t on ch.featureid=t.featureid
)
select featureid, huc12, intersect_area, catchment_area, intersect_area/catchment_area as fraction_area
from x
order by fraction_area













group by t.featureid
order by w.huc12 desc;


-- Flowlines with a single overlap get assigned that HUC.



---------------------------------------------------------------------------------------------------------------------------------------
-- Jeff's Example Code
-- ===================

-- Check to see fraction of catchment that falls into it's assigned huc 12
with x as (
  select
    ch.featureid,
    ch.huc12,
    ST_Area(ST_Intersection(ST_Transform(w.geom, 2163), ST_Transform(c.geom, 2163))) as intersect_area,
    ST_Area(ST_Transform(c.geom, 2163)) as catchment_area
  from catchment_huc12 ch
  left join wbdhu12 w on ch.huc12=w.huc12
  left join catchments c on ch.featureid=c.featureid
)
select featureid, huc12, intersect_area, catchment_area, intersect_area/catchment_area as fraction_area
from x
order by fraction_area

-- Count how many HUCs the catchment overlaps with
select c.featureid, count(w.*) as n_huc12
from catchments as c
inner join wbdhu12 as w
on ST_Overlaps(ST_Transform(w.geom, 2163), ST_Transform(c.geom, 2163))
group by c.featureid
order by n_huc12 desc

-- this should be converted to ST_Intersects for lines

---------------------------------------------------------------------------------------------------------------------------------------










-- Join huc 12s and catchments into one table
--select t.featureid, count(w.*) as n_huc12 into temporary intersections
--from temp.trunchc as t
--inner join wbdhu12 as w
--on ST_Intersects(ST_Transform(w.geom, 2163), ST_Transform(t.geom, 2163))
--group by t.featureid
--order by n_huc12 desc;









select column_name, data_type from information_schema.columns where table_name = 'wbdhu12';
select column_name, data_type from information_schema.columns where table_name = 'catchment_huc12';
select column_name, data_type from information_schema.columns where table_name = 'cathuc12';




























