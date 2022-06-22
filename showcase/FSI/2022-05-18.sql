CREATE Database md

USE md

DROP TABLE if exists table_ext_geoloc ;

CREATE EXTERNAL TABLE if not exists table_ext_geoloc (
    atmid INT,
    city STRING,
    lat DOUBLE,
    lon DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "s3a://try01cdpsaas-cdp-private-default-yyfn0q0/user/mdaeppen/geolocation"
TBLPROPERTIES ("skip.header.line.count"="1", "external"="true");

SELECT * FROM table_ext_geoloc ;


CREATE EXTERNAL TABLE if not exists table_ext_cc_trx_fx (
    trxtsd BIGINT,
    creditcardid STRING,
    creditcardtype STRING,
    atmid INT,
    atmlocname STRING,
    amount DOUBLE,
    fx STRING,
    targetfx STRING,
    fxtsd BIGINT,
    fxrate DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "s3a://try01cdpsaas-cdp-private-default-yyfn0q0/user/mdaeppen/cctrx";

SELECT * FROM table_ext_cc_trx_fx ;


CREATE TABLE if not exists table_managed_cc_trx_fx
STORED AS PARQUET
as select * from table_ext_cc_trx_fx;

SELECT * FROM table_managed_cc_trx_fx
LIMIT 5;


CREATE VIEW if not exists view_atmid_1
as select * from table_managed_cc_trx_fx where atmid = 1 ;

CREATE VIEW if not exists view_atmid_6
as select * from table_managed_cc_trx_fx where atmid = 6 ;

CREATE VIEW if not exists view_atmid_9
as select * from table_managed_cc_trx_fx where atmid = 9 ;



CREATE TABLE if not exists table_managed_geoloc
STORED AS PARQUET
as select * from table_ext_geoloc;

SELECT * FROM table_managed_geoloc
LIMIT 3;


SELECT cctrxfx.atmid,
 geoloc.city, 
 geoloc.lat, 
 geoloc.lon, 
 count(*) as anzahl
FROM table_managed_cc_trx_fx cctrxfx, table_managed_geoloc geoloc 
WHERE cctrxfx.atmid = geoloc.atmid 
GROUP BY atmid, city, lat, lon
ORDER BY anzahl DESC;