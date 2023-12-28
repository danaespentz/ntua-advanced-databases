from load_data import *

#### ==== QUERY 3 ====

# Να βρεθεί η καταγωγή (descent) των καταγεγραμμένων θυμάτων εγκλημάτων στο Los Angeles 
# για το έτος 2015 στις 3 περιοχές (ZIP Codes) με το υψηλότερο και τις 3 περιοχές (ZIP Codes) 
# με το χαμηλότερο εισόδημα ανά νοικοκυριό.
# -----------------------------------------------

Crime_Data3.createOrReplaceTempView("CrimeDataView3")
Income.createOrReplaceTempView("IncomeView")

# === SQL ===
sql_str = """
    WITH TopZipCodes AS (
        SELECT
            ZIP,
            Income
        FROM
            IncomeView
        ORDER BY
            Income DESC
        LIMIT 3
    ),
    BottomZipCodes AS (
        SELECT
            ZIP,
            Income
        FROM
            (SELECT
                ZIP,
                Income
            FROM
                IncomeView
            ORDER BY
                Income ASC
            LIMIT 3) AS bottom_incomes
        ORDER BY
            Income ASC
    )
    SELECT
        C.`Vict Descent`,
        COUNT(*) AS victim_count
    FROM
        CrimeDataView3 C
    INNER JOIN
        (SELECT ZIP FROM TopZipCodes UNION ALL SELECT ZIP FROM BottomZipCodes) AS RankedZipCodes
        ON C.ZIP = RankedZipCodes.ZIP
    GROUP BY
        C.`Vict Descent`
    ORDER BY
        victim_count DESC;
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

top_bottom_zip = Income.orderBy('Income') \
    .select('ZIP') \
    .limit(3).union(
        Income.orderBy(desc('Income'))
        .select('ZIP')
        .limit(3)
    )

result = Crime_Data3.join(top_bottom_zip, 'ZIP', 'inner') \
    .groupBy('Vict Descent') \
    .agg(count('*').alias('crime_total')) \
    .orderBy(col('crime_total').desc())

total_time += time.time() - start_time

result.show()
print('Total time for DataFrame: ', str(total_time), 'sec')
f.write('Time for Q3: ' + str(total_time) + '\n')
f.close()