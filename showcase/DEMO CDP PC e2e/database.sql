create DATABASE if not exists iot;

USE iot;

DROP TABLE iot.table_ext_geoloc;

CREATE EXTERNAL TABLE if not exists iot.table_ext_geoloc (
sensor_id INT
,city STRING
,lat DOUBLE
, lon DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_geo"
tblproperties ("skip.header.line.count"="1", "external"="true");

SELECT * FROM iot.table_ext_geoloc
LIMIT 3;

CREATE TABLE if not exists iot.table_int_geoloc
STORED AS orc
as select * from iot.table_ext_geoloc;


SELECT * FROM iot.table_int_geoloc
LIMIT 3;


DROP TABLE iot.table_ext_iot ;

CREATE EXTERNAL TABLE if not exists iot.table_ext_iot (
sensor_id INT
,field_2 INT
,field_3 INT
,field_4 INT
,field_5 DOUBLE
,field_6 INT
,field_7 DOUBLE
,field_8 INT
,field_9 DOUBLE
,field_10 INT
,field_11 DOUBLE
,field_12 INT
,field_13 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_iot";

SELECT * FROM iot.table_ext_iot
LIMIT 3;

CREATE TABLE if not exists iot.table_int_iot
STORED AS orc
as select * from iot.table_ext_iot;

SELECT * FROM iot.table_int_iot
LIMIT 3;

DROP VIEW iot.view_sensor_10 ;
DROP VIEW iot.view_sensor_88 ;

CREATE VIEW if not exists iot.view_sensor_10
as select * from iot.table_int_iot where sensor_id = 10 ;

CREATE VIEW if not exists iot.view_sensor_88
as select * from iot.table_int_iot where sensor_id = 88 ;


SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon FROM iot.table_int_iot iot, iot.table_int_geoloc geoloc
WHERE iot.sensor_id = geoloc.sensor_id
Limit 3;

SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon ,count(*) as counter FROM iot.table_int_iot iot , iot.table_int_geoloc geoloc
WHERE iot.sensor_id = geoloc.sensor_id
GROUP BY iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon;


CREATE MATERIALIZED VIEW IF NOT EXISTS iot.view_mat_join
STORED AS ORC
AS SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon  FROM iot.table_int_iot iot, iot.table_int_geoloc geoloc WHERE iot.sensor_id = geoloc.sensor_id ;

SELECT * FROM iot.view_mat_join;