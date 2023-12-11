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
for i in range(n_iter):
    start_time = time.time()
    result = Crime_Data2.filter(Crime_Data2['Premis Desc'] == 'STREET') \
        .groupBy('Time OCC') \
        .agg(F.count('*').alias('crime_total')) \
        .orderBy(F.col('crime_total').desc())
    result.count()
    total_time += time.time() - start_time

result.show()
print('Average Total time for DataFrame: ', str(total_time/n_iter), 'sec')
f.write('Average Time for Q1: ' + str(total_time/n_iter) + '\n')
f.close()