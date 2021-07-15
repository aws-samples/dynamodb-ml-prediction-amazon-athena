--
-- This Athena statement creates a table entry in the Glue catalogue
-- The data format is a TAB-seperated text file without header row
--
CREATE EXTERNAL TABLE "${Database}"."loisville_ky_neighborhoods"(
  `objectid` int, 
  `nh_code` int, 
  `nh_name` string, 
  `shapearea` double, 
  `shapelen` double, 
  `bb_west` double, 
  `bb_south` double, 
  `bb_east` double, 
  `bb_north` double, 
  `shape` string, 
  `cog_longitude` double, 
  `cog_latitude` double)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://${S3BucketExternalCode}/loisville_ky_neighborhoods/'
TBLPROPERTIES (
  'has_encrypted_data'='false'
  )
