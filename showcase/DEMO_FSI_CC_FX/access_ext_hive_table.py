## Get Data via HIVE external table
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
spark.sql("SHOW TABLES in bank").show(truncate=False)

spark.sql("SELECT creditcardid, creditcardtype, atmlocname,atmid, amount FROM bank.table_ext_cc_trx_fx").show()