
CREATE SCHEMA temp;


-- Flowlines
-- ---------
CREATE TEMPORARY TABLE truncTable (featureid  bigint);

COPY truncTable
	FROM '/home/kyle/huccat/truncTableFinal.csv'
	WITH CSV HEADER;

ALTER TABLE truncTable RENAME FEATUREID TO selectid;

SELECT 
     src.*
	 INTO temp.trunc_hc
  FROM 
     gis.truncated_flowlines src, 
     truncTable sel 
  WHERE 
     src.featureid = sel.selectid;
	
	
-- Catchments
-- ----------
CREATE TEMPORARY TABLE catTable (featureid  bigint);

COPY catTable
	FROM '/home/kyle/huccat/catTableFinal.csv'
	WITH CSV HEADER;

ALTER TABLE catTable RENAME FEATUREID TO selectid;

SELECT 
     src.*
	 INTO temp.cat_hc
  FROM 
     gis.catchments src, 
     catTable sel 
  WHERE 
     src.featureid = sel.selectid;
		
	
	
	
	
-------------------------------------------------------------------------------
-------------------------------  WORKSPACE	-----------------------------------
-------------------------------------------------------------------------------
	
-- HUC 12
-- ------
CREATE TEMPORARY TABLE hucTable (huc12  varchar(12));

COPY hucTable
	FROM '/home/kyle/huccat/nhdhrdv2_huc12.csv'
	WITH CSV HEADER;

ALTER TABLE hucTable RENAME huc12 TO selectid;

SELECT 
     src.*
	 INTO temp.huc12_hc
  FROM 
     gis.wbdhu12 src, 
     hucTable sel 
  WHERE 
     src.huc12 LIKE sel.selectid;
	
	
	
	
	
	
SELECT 
     huc12
  FROM 
     gis.wbdhu12
  WHERE 
     huc12 LIKE '020402070401';


select max(length(huc12)) from wbdhu12;
select max(length(selectid)) from hucTable;
select column_name, data_type from information_schema.columns where table_name = 'wbdhu12';
