from load_data import *

#### ==== QUERY 4 ====

# Kατά πόσον τα εγκλήματα που καταγράφονται στην πόλη του Los Angeles αντιμετωπίζονται από το πλησιέστερο 
# στον τόπο εγκλήματος αστυνομικό τμήμα ή όχι. 
# -----------------------------------------------

Crime_Data4.createOrReplaceTempView("CrimeDataView4")

# === SQL ===
sql_str = """
    SELECT 
        YEAR(C.`Date Rptd`) AS year,
        ROUND(AVG(C.`distance`), 4) AS average_distance,
        COUNT(*) AS crime_total
    FROM 
        CrimeDataView4 C
    WHERE 
        C.`Weapon Used Cd` LIKE '1%%'
    GROUP BY
        C.`Date Rptd`
    ORDER BY 
        YEAR(C.`Date Rptd`) ASC 
"""
sql_str2 = """
    SELECT 
        C.`AREA NAME` AS division,
        ROUND(AVG(C.`distance`), 4) AS average_distance,
        COUNT(*) AS crime_total
    FROM 
        CrimeDataView4 C
    GROUP BY
        C.`AREA NAME`
    ORDER BY 
        crime_total DESC 
"""
sql_str3 = """
    SELECT 
        YEAR(C.`Date Rptd`) AS year,
        ROUND(AVG(C.`min_distance`), 4) AS average_distance,
        COUNT(*) AS crime_total
    FROM 
        CrimeDataView4 C
    WHERE 
        C.`Weapon Used Cd` LIKE '1%%'
    GROUP BY
        C.`Date Rptd`
    ORDER BY 
        YEAR(C.`Date Rptd`) ASC 
"""
sql_str4 = """
    SELECT 
        C.`AREA NAME` AS division,
        ROUND(AVG(C.`min_distance`), 4) AS average_distance,
        COUNT(*) AS crime_total
    FROM 
        CrimeDataView4 C
    GROUP BY
        C.`AREA NAME`
    ORDER BY 
        crime_total DESC 
"""

# Run the query
start_time = time.time()
res = spark.sql(sql_str)

# Show the result
res.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')

# Run the query 2
start_time = time.time()
res2 = spark.sql(sql_str2)

# Show the result2
res2.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')

# Run the query 3
start_time = time.time()
res3 = spark.sql(sql_str3)

# Show the result3
res3.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')

# Run the query 4
start_time = time.time()
res4 = spark.sql(sql_str4)

# Show the result4
res4.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')

# === DataFrame ===
start_time = time.time()
result = Crime_Data4.filter(col("Weapon Used Cd").like('1%')) \
    .groupBy(year("Date Rptd").alias("year")) \
    .agg({"distance": "avg", "*": "count"}) \
    .orderBy("year", ascending=True) \
    .withColumnRenamed("avg(distance)", "avg_distance") \
    .withColumnRenamed("count(1)", "crime_total") \
    .withColumn("avg_distance", round(col("avg_distance"), 4))

total_time = time.time() - start_time
result.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q4aa: ' + str(total_time) + '\n')

start_time = time.time()
result2 = Crime_Data4.groupBy("AREA NAME") \
    .agg({"distance": "avg", "*": "count"}) \
    .orderBy("count(1)", ascending=False) \
    .withColumnRenamed("avg(distance)", "avg_distance") \
    .withColumnRenamed("count(1)", "crime_total") \
    .withColumnRenamed("AREA NAME", "division") \
    .withColumn("avg_distance", round(col("avg_distance"), 4))

total_time = time.time() - start_time
result2.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q4ab: ' + str(total_time) + '\n')

start_time = time.time()
result3 = Crime_Data4.filter(col("Weapon Used Cd").like('1%')) \
    .groupBy(year("Date Rptd").alias("year")) \
    .agg({"min_distance": "avg", "*": "count"}) \
    .orderBy("year", ascending=True) \
    .withColumnRenamed("avg(min_distance)", "avg_distance") \
    .withColumnRenamed("count(1)", "crime_total") \
    .withColumn("avg_distance", round(col("avg_distance"), 4))

total_time = time.time() - start_time
result3.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q4ba: ' + str(total_time) + '\n')

start_time = time.time()
result4 = Crime_Data4.groupBy("AREA NAME") \
    .agg({"min_distance": "avg", "*": "count"}) \
    .orderBy("count(1)", ascending=False) \
    .withColumnRenamed("avg(min_distance)", "avg_distance") \
    .withColumnRenamed("count(1)", "crime_total") \
    .withColumnRenamed("AREA NAME", "division") \
    .withColumn("avg_distance", round(col("avg_distance"), 4))

total_time = time.time() - start_time
result4.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q4bb: ' + str(total_time) + '\n')
f.close()
