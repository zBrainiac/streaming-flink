

```SQL
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


SELECT * from bank.table_ext_cc_trx_fx ;

DROP TABLE table_ext_cc_trx_fx_part;


CREATE EXTERNAL TABLE if not exists bank.table_ext_cc_trx_fx_part (
trxtsd BIGINT
,creditcardid STRING
,creditcardtype STRING
,atmlocname STRING
,amount DOUBLE
,fx STRING
,targetfx STRING
,fxtsd BIGINT
,fxrate DOUBLE)
PARTITIONED BY (atmid INT)
LOCATION "s3a://gd01-uat2/gd01-demo/mdaeppen/data_fsi_cc_trx_part";

SHOW PARTITIONS bank.table_ext_cc_trx_fx_part ;


INSERT overwrite TABLE table_ext_cc_trx_fx_part partition (atmid)
 SELECT * FROM table_ext_cc_trx_fx;
 
SELECT * FROM table_ext_cc_trx_fx_part;
 
ALTER TABLE table_ext_cc_trx_fx_part DROP IF EXISTS PARTITION(atmid<>0) ;

SHOW PARTITIONS bank.table_ext_cc_trx_fx_part ;

MSCK REPAIR TABLE table_ext_cc_trx_fx_part;

SHOW PARTITIONS bank.table_ext_cc_trx_fx_part ;

SELECT * FROM table_ext_cc_trx_fx_part;
```
