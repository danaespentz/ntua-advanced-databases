from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, StructField, StructType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col, round, udf, dense_rank, date_format, year, month, count, desc
from pyspark.sql.window import Window
import time, datetime
import csv

spark = SparkSession.builder \
    .appName("Crime_Data") \
    .getOrCreate()
print("Spark session created")

data_schema = StructType([
    StructField("Date Rptd", DateType()),
    StructField("DATE OCC", DateType()),
    StructField("Vict Age", IntegerType()),
    StructField("LAT", FloatType()),
    StructField("LON", FloatType()),
])

data_schema2 = StructType([
    StructField("Time OCC", StringType()),
    StructField("Premis Desc", StringType()),
])

data_schema3 = StructType([
    StructField("Date Rptd", DateType()),
    StructField("Vict Descent", StringType()),
    StructField("ZIP", IntegerType()),
    StructField("Income", IntegerType()),
])

income_schema = StructType([
    StructField("ZIP", IntegerType()),
    StructField("LOC", StringType()),
    StructField("Income", IntegerType()),
])

data_schema4 = StructType([
    StructField("Date Rptd", DateType()),
    StructField("AREA ", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Weapon Used Cd", FloatType()),
    StructField("distance", FloatType()),
])

Crime_Data_from_2020_to_Present = spark.read.csv("hdfs://okeanos-master:54310/data/Selected_Crime_Data_from_2020_to_Present.csv", header=True, schema=data_schema)
Crime_Data_from_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected_Crime_Data_from_2010_to_2019.csv", header=True, schema=data_schema)
Crime_Data = Crime_Data_from_2020_to_Present.union(Crime_Data_from_2010_to_2019)

Crime_Data2_from_2020_to_Present = spark.read.csv("hdfs://okeanos-master:54310/data/Selected2_Crime_Data_from_2020_to_Present.csv", header=True, schema=data_schema2)
Crime_Data2_from_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected2_Crime_Data_from_2010_to_2019.csv", header=True, schema=data_schema2)
Crime_Data2 = Crime_Data2_from_2020_to_Present.union(Crime_Data2_from_2010_to_2019)

Crime_Data3 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected3_Crime_Data_2015.csv", header=True, schema=data_schema3)
Income = spark.read.csv("hdfs://okeanos-master:54310/data/income2015.csv", header=True, schema=income_schema)

Crime_Data4_from_2020_to_Present = spark.read.csv("hdfs://okeanos-master:54310/data/Selected4_Crime_Data_from_2020_to_Present.csv", header=True, schema=data_schema4)
Crime_Data4_from_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected4_Crime_Data_from_2010_to_2019.csv", header=True, schema=data_schema4)
Crime_Data4 = Crime_Data4_from_2020_to_Present.union(Crime_Data4_from_2010_to_2019)

# Write Time execution results
f = open("execution_time.txt", "a")

# Print number of rows and column types
print("Number of rows:", Crime_Data.count())
print("Column types:")
Crime_Data.printSchema()
