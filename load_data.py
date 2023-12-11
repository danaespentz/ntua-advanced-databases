from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, StructField, StructType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col, udf, dense_rank, date_format, year, month, count
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

Crime_Data_from_2020_to_Present = spark.read.csv("hdfs://okeanos-master:54310/data/Selected_Crime_Data_from_2020_to_Present.csv", header=False, schema=data_schema)
Crime_Data_from_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected_Crime_Data_from_2010_to_2019.csv", header=False, schema=data_schema)
Crime_Data = Crime_Data_from_2020_to_Present.union(Crime_Data_from_2010_to_2019)

Crime_Data2_from_2020_to_Present = spark.read.csv("hdfs://okeanos-master:54310/data/Selected2_Crime_Data_from_2020_to_Present.csv", header=False, schema=data_schema2)
Crime_Data2_from_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/Selected2_Crime_Data_from_2010_to_2019.csv", header=False, schema=data_schema2)
Crime_Data2 = Crime_Data2_from_2020_to_Present.union(Crime_Data2_from_2010_to_2019)

# Write Time execution results
f = open("execution_time.txt", "a")

# Query runtime parameters
n_iter = 10

# Print number of rows and column types
print("Number of rows:", Crime_Data.count())
print("Column types:")
Crime_Data.printSchema()