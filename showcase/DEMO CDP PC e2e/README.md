# e2e Demo flow across multiple CDP PC experiences

![e2e demo flow](../../images/showcase_CDP-PC_e2e_flow.png?raw=true "e2e demo flow")


## guideline
1. In Data Hub  
   HUE - upload two files into an s3 bucket  
   a.) lookup table _sensor_io_ to _geo-location - "s3a://demo-aws-2/user/mdaeppen/data_geo"  
   b.) historical iot data - "s3a://demo-aws-2/user/mdaeppen/data_iot"
   

2. In (Data Hub or) CDW  
   HUE - HIVE SQL Editor [follow the script database.sql](database.sql)  
   a.) create database iot
   
```
CREATE DATABASE if not exists iot;

```

   b.) create external tables: iot.table_ext_geoloc & iot.table_ext_iot  
```
CREATE EXTERNAL TABLE if not exists iot.table_ext_geoloc (
sensor_id INT
,city STRING
,lat DOUBLE
, lon DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_geo"
tblproperties ("skip.header.line.count"="1", "external"="true");


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
```
   c.) create managed tables: iot.table_managed_geoloc & table_managed_iot  
```
CREATE TABLE if not exists iot.table_managed_geoloc
STORED AS orc
as select * from iot.table_ext_geoloc;


CREATE TABLE if not exists iot.table_managed_iot
STORED AS orc
as select * from iot.table_ext_iot;
```
   d.) create some views & materialized views 
```
CREATE VIEW if not exists iot.view_sensor_10
as select * from iot.table_managed_iot where sensor_id = 10 ;


CREATE MATERIALIZED VIEW IF NOT EXISTS iot.view_matwerialized_join_iot_geo
STORED AS ORC
AS SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon  FROM iot.table_managed_iot iot, iot.table_managed_geoloc geoloc WHERE iot.sensor_id = geoloc.sensor_id ;
```
   e.) run some select's  
```
SELECT iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon ,count(*) as counter FROM iot.table_managed_iot iot , iot.table_managed_geoloc geoloc
WHERE iot.sensor_id = geoloc.sensor_id
GROUP BY iot.sensor_id, geoloc.city, geoloc.lat, geoloc.lon;
```
   

3. In CML (browser or remote IDE)  
   Build a PySpark job [sample](ConnectToCDW_SQL.py) in CML that:  
   a.) Shows databases and Tables of Database 'iot'  
   b.) joins the two external tables  
   c.) write result back to s3 - s3a://demo-aws-2/user/mdaeppen/data_out/joined_iot_geoloc  
   
   
4. In CDE  
   Create a new job based on the before writen PySpart job [sample](ConnectToCDW_SQL.py)  


## remote IDE
```
cd infra
./cdswctl login -n <name> -u  https://ml-18a296af-d86.demo-aws.ylcu-atmi.cloudera.site/mdaeppen/vizapp -y <API Key>
```
Start secure endpoint
```
./cdswctl ssh-endpoint -p vizapp -m 4 -c 2
```

Setup SSL host
```
Host cdsw-public
    HostName localhost
    IdentityFile ~/.ssh/id_rsa_cml
    User cdsw
    Port 4869
```