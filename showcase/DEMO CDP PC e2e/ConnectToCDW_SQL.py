## Get Data ftom CDW
# This script grabs the data file from the Cloud Storage location // HIVE Meta store to read the structure

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("PythonSQL") \
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-2//") \
    .getOrCreate()

spark.sql("show databases").show()
spark.sql("show tables in iot").show()

spark.sql("select * from iot.geoloc").show()
spark.sql("select * from iot.iot").show()
spark.sql("select * from iot.view_sensor_88").show()
