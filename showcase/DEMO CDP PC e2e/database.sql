create database iot;

USE iot;

DROP TABLE iot.geoloc;

CREATE EXTERNAL TABLE iot.geoloc (
`sensor_id` INT,
`city` STRING,
`lat` DOUBLE,
 `lon` DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_geo"

tblproperties ("skip.header.line.count"="1", "external"="true");

SELECT * FROM iot.geoloc
LIMIT 3;


DROP TABLE iot.iot ;

CREATE EXTERNAL TABLE iot.iot(sensor_id INT
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
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_iot"

SELECT * FROM iot.iot
LIMIT 3;


SELECT * FROM iot , geoloc
WHERE iot.sensor_id = geoloc.sensor_id
Limit 3;

SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon ,count(*) as counter FROM iot , geoloc
WHERE iot.sensor_id = geoloc.sensor_id
GROUP BY iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon;


DROP VIEW iot.view_sensor_10 ;
DROP VIEW iot.view_sensor_88 ;

CREATE VIEW if not exists view_sensor_10
as select * from iot.iot where sensor_id = 10 ;

CREATE VIEW if not exists view_sensor_88
as select * from iot.iot where sensor_id = 88 ;