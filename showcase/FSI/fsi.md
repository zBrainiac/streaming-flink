


CREATE DATABASE if not exists bank_a;

USE bank_a;

DROP table bank_a.creditcardtrxfx;

CREATE EXTERNAL TABLE if not exists bank_a.creditcardtrxfx (
trxtsd BIGINT
,creditcardid STRING
,creditcardtype STRING
,shopid STRING
,shopname STRING
,amount DOUBLE
,fx STRING
,targetfx STRING
,fxtsd STRING
,fxrate DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION "s3a://demo-aws-2/user/mdaeppen/data_FSI_CC_TRX/";

SELECT* FROM creditcardtrxfx;