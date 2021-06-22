import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession \
    .builder \
    .appName("PythonSQL") \
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-2//") \
    .getOrCreate()

## Get data from geo location data dataset
print('load data from "geo location" data dataset')
start_time = time.time()

geolocation_schema = StructType() \
    .add("atmid", IntegerType(), True) \
    .add("city", StringType(), True) \
    .add("lat", DoubleType(), True) \
    .add("lon", DoubleType(), True)

df_geolocation_data_clean = spark.read.format("csv") \
    .option("header", True) \
    .schema(geolocation_schema) \
    .load("s3a://demo-aws-2/user/mdaeppen/data_geo/")

df_geolocation_data_clean.printSchema()

df_geolocation_data_clean.show(n=5, truncate=False)
print("Anzahl Standorte:",df_geolocation_data_clean.count())

print("--- %s 'geo location data cleansing' in seconds ---" % (time.time() - start_time))