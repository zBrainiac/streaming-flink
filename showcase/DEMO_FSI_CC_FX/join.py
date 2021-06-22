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

creditcardtrx_schema = StructType() \
    .add("trxtsd", StringType(), True) \
    .add("creditcardid", StringType(), True) \
    .add("creditcardtype", StringType(), True) \
    .add("atmid", IntegerType(), True) \
    .add("shopname", StringType(), True) \
    .add("amount", DoubleType(), True) \
    .add("targetfx", StringType(), True) \
    .add("fxtsd", StringType(), True) \
    .add("fxrate", DoubleType(), True)

geolocation_schema = StructType() \
    .add("atmid", IntegerType(), True) \
    .add("city", StringType(), True) \
    .add("lat", DoubleType(), True) \
    .add("lon", DoubleType(), True)


df_creditcardtrx_data_clean = spark.read.format("csv") \
    .option("header", True) \
    .schema(creditcardtrx_schema) \
    .load("s3a://demo-aws-2/user/mdaeppen/data_FSI_CC_TRX/")

df_creditcardtrx_data_clean.printSchema()

df_creditcardtrx_data_clean.show(n=5, truncate=False)
print("Anzahl Credit Card trx:",df_creditcardtrx_data_clean.count())


df_geolocation_data_clean = spark.read.format("csv") \
    .option("header", True) \
    .schema(geolocation_schema) \
    .load("s3a://demo-aws-2/user/mdaeppen/data_geo/")

df_geolocation_data_clean.printSchema()

df_geolocation_data_clean.show(n=5, truncate=False)



df_creditcardtrx_data_clean.join(df_geolocation_data_clean, \
                                 df_creditcardtrx_data_clean.atmid == df_geolocation_data_clean.atmid, \
                                 "inner").select(df_creditcardtrx_data_clean["*"], \
                                                 df_geolocation_data_clean["city"], \
                                                 df_geolocation_data_clean["lat"], \
                                                 df_geolocation_data_clean["lon"]) \
    .show(truncate=False)



print("--- %s 'geo location data cleansing' in seconds ---" % (time.time() - start_time))