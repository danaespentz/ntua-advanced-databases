from load_data import *

#### ==== QUERY 1 ====

# Να βρεθούν, για κάθε έτος, οι 3 μήνες με τον υψηλότερο αριθμό καταγεγραμμένων εγκλημάτων.
# -----------------------------------------------

Crime_Data.createOrReplaceTempView("CrimeDataView")

# === SQL ===
sql_str = """
    WITH RankedCrimeData AS (
        SELECT 
            YEAR(C.`Date Rptd`) AS year,
            MONTH(C.`Date Rptd`) AS month,
            COUNT(*) AS crime_total,
            ROW_NUMBER() OVER (PARTITION BY YEAR(C.`Date Rptd`) ORDER BY COUNT(*) DESC) AS row_num
        FROM 
            CrimeDataView C
        GROUP BY 
            YEAR(C.`Date Rptd`), 
            MONTH(C.`Date Rptd`)
    )
    SELECT 
        year,
        month,
        crime_total
    FROM 
        RankedCrimeData
    WHERE 
        row_num <= 3
    ORDER BY 
        year ASC,
        crime_total DESC 
"""

# Run the query
start_time = time.time()
res = spark.sql(sql_str)

# Show the result
res.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')

# === DataFrame ===
total_time = 0
start_time = time.time()
windowSpec = Window.partitionBy("year").orderBy(col("crime_total").desc())
ranked = Crime_Data.groupBy(year("Date Rptd").alias("year"), month("Date Rptd").alias("month")) \
    .agg(count("*").alias("crime_total")) \
    .withColumn("rank", dense_rank().over(windowSpec))
result = ranked.filter(col("rank") <= 3).orderBy(col("year").asc(), col("crime_total").desc())  
result.count()
total_time += time.time() - start_time

result.show()
print('Average Total time for DataFrame: ', str(total_time/n_iter), 'sec')
f.write('Average Time for Q1: ' + str(total_time/n_iter) + '\n')
f.close()
