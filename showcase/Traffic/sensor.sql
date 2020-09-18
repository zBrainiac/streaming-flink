CREATE TABLE sensors_enhanced
(
 type STRING,
 subtype STRING,
 sensor_ts TIMESTAMP,
 sensor_id INT,
 temp DOUBLE,
 rain_level DOUBLE,
 visibility_level DOUBLE,
 location STRING,
 lat DOUBLE,
 long DOUBLE,
 PRIMARY KEY (sensor_id, sensor_ts)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');
