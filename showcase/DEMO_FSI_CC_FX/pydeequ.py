import pydeequ

import pyspark
from pyspark.sql import SparkSession, Row



spark = (SparkSession
.builder
.config("spark.jars.packages", 'com.amazon.deequ:deequ:1.1.0_spark-3.0-scala-2.12')
.config("spark.jars.excludes", pydeequ.f2j_maven_coord)
.getOrCreate())

df = spark.read.csv("data/testCSV_short.csv")


df.printSchema()


print(df)