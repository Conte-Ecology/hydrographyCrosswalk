CREATE TABLE gis.region_boundary (
  featureid  bigint,
  geom);

  
CREATE TABLE gis.region_boundary (
    fid  int, 
    id   int
);

ALTER TABLE gis.region_boundary ADD COLUMN geom geometry(MULTIPOLYGON,4326);  



cd /home/kyle/scripts/db/gis/region_boundary
./import_boundary.sh sheds_new /home/kyle/data/gis/region_boundary




select ST_Transform(c.geom, 2163) from temp.cat_hc c;


select ST_Transform(w.geom, 2163) from wbdhu12 w;

select ST_Transform(w.geom, 2163) into temp.wbdhu12_prj from wbdhu12 w;


select ST_Envelope(ST_Transform(w.geom, 2163)) from temp.cat_hc w;




SELECT id from 
select ST_Envelope(ST_Transform(w.geom, 2163)) from gis.regionboundary w;


select w.huc12 into temp.test2
from wbdhu12 w, temp.cat_hc c
where ST_Intersects(ST_Transform(w.geom, 2163), ST_Envelope(ST_Transform(c.geom, 2163)));



select ST_Envelope(geometry g1)





