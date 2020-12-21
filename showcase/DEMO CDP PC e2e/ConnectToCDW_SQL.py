## Get Data from CDW
# This script grabs the data file from the Cloud Storage location // HIVE Meta store to read the structure
import os
import sys

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("PythonSQL") \
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-2//") \
    .getOrCreate()

spark.sql("SHOW databases").show()
spark.sql("SHOW TABLES in iot").show()

spark.sql("select * FROM iot.table_ext_geoloc").show()
spark.sql("SELECT iot.sensor_id, iot.field_2, iot.field_3, iot.field_4, iot.field_5, geoloc.city, geoloc.lat, geoloc.lon \
    FROM iot.table_ext_iot iot, iot.table_ext_geoloc geoloc \
    WHERE iot.sensor_id = geoloc.sensor_id").show()

df = spark.sql("SELECT iot.sensor_id, iot.field_2, iot.field_3, iot.field_4, iot.field_5, geoloc.city, geoloc.lat, geoloc.lon \
    FROM iot.table_ext_iot iot, iot.table_ext_geoloc geoloc \
    WHERE iot.sensor_id = geoloc.sensor_id")

# dataframe loaded
df.write.csv("s3a://demo-aws-2/user/mdaeppen/data_out/joined_iot_geoloc", header='True', mode='overwrite')

# dataframe saved into s3

# dop table
spark.sql("DROP TABLE IF EXISTS iot.table_managed_join") 


# create new external table
df.write.option('path','s3a://demo-aws-2/user/mdaeppen/data_out/joined_iot_geoloc') \
    .saveAsTable('iot.table_managed_join')

# select same data form the new table
spark.sql("SELECT sensor_id, city,lat, lon, count(*) as count FROM iot.table_managed_join \
           GROUP BY sensor_id, city,lat, lon \
           ORDER BY count DESC").show()