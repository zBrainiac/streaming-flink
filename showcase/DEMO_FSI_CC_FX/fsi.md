

```SQL
CREATE DATABASE if not exists bank_a;

USE bank_a;

DROP TABLE bank_a.creditcardtrxfx;

CREATE EXTERNAL TABLE if not exists bank_a.table_ext_creditcardtrx_fx (
trxtsd BIGINT
,creditcardid STRING
,creditcardtype STRING
,shopid INT
,shopname STRING
,amount DOUBLE
,fx STRING
,targetfx STRING
,fxtsd BIGINT
,fxrate DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_FSI_CC_TRX";

SELECT * FROM bank_a.table_ext_creditcardtrx_fx
LIMIT 5;

DROP TABLE bank_a.table_ext_geoloc;

CREATE EXTERNAL TABLE if not exists bank_a.table_ext_geoloc (
sensor_id INT
,city STRING
,lat DOUBLE
, lon DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_geo"
tblproperties ("skip.header.line.count"="1", "external"="true");


SELECT * FROM bank_a.table_ext_creditcardtrx_fx ccfx, bank_a.table_ext_geoloc geo
WHERE ccfx.shopid = geo.sensor_id;
```
