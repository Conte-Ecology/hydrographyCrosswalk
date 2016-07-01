
--=========================================================================================
--                                   QAQC
--=========================================================================================

-- Export cat/huc relationship list
psql -d sheds_new -c"COPY cathuc12 TO STDOUT WITH CSV HEADER" > /home/kyle/hydrography_crosswalk/huc_to_catchments/cathuc12_06232016.csv


-- Export headwaters with multiple huc intersections in order to 
-- 	get a better look at where some issues might exist (manual check)
select * into temp.hw_issues
  from temp.match_count_lines 
  where type_id like 'headwater' 
    and n_huc12 > 1;

-- Export 
psql -d sheds_new -c"COPY temp.hw_issues TO STDOUT WITH CSV HEADER" > /home/kyle/hydrography_crosswalk/huc_to_catchments/hw_issues_06232016.csv
	
	
	
	
	
	
	
	
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



















