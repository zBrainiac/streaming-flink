CREATE TABLE sensors_enhanced(
 sensor_id INT,
 sensor_ts TIMESTAMP,
 type STRING,
 subtype STRING,
 temp DOUBLE,
 rain_level DOUBLE,
 visibility_level DOUBLE,
 city STRING,
 lat DOUBLE,
 long DOUBLE,
 PRIMARY KEY (sensor_id, sensor_ts)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');
