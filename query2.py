from load_data import *

#### ==== QUERY 2 ====

#Να ταξινομημηθούν τα τμήματα της ημέρας ανάλογα με τις καταγραφές εγκλημάτων που έλαβαν χώρα στο δρόμο (STREET), με φθίνουσα σειρά. 
# Θεωρείστε τα εξής τμήματα μέσα στη μέρα:
    # Morning:5.00–11.59
    # Noon:12.00–16.59
    # Afternoon:17.00–20.59
    # Night:21.00–3.59
# -----------------------------------------------

Crime_Data2.createOrReplaceTempView("CrimeDataView2")

# === SQL ===
sql_str = """
    SELECT 
        C.`Time OCC` AS time,
        COUNT(*) AS crime_total
    FROM 
        CrimeDataView2 C
    WHERE C.`Premis Desc`=='STREET'
    GROUP BY 
        C.`Time OCC`
    ORDER BY 
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
result = Crime_Data2.filter(Crime_Data2['Premis Desc'] == 'STREET') \
        .groupBy('Time OCC') \
        .agg(count('*').alias('crime_total')) \
        .orderBy(col('crime_total').desc())
result.count()
total_time += time.time() - start_time

result.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q2: ' + str(total_time) + '\n')

# === RDD ===
total_time = 0
start_time = time.time()

final_rdd = Crime_Data2.rdd.filter(lambda row: row['Premis Desc'] == 'STREET')\
                        .map(lambda row: (row['Time OCC'], 1))\
                        .reduceByKey(lambda x, y: x + y)\
                        .map(lambda x: (x[1], x[0]))\
                        .sortByKey(False)
                        
total_time += time.time() - start_time

for x in final_rdd.collect():
    print(x)

print('Total time for RDD: ', str(total_time), 'sec')
f.write('Time for Q3-RDD: ' + str(total_time) + '\n')
f.close()
