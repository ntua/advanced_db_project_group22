from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round, rank, trim, upper
from pyspark.sql.window import Window
import time

# Δημιουργία SparkSession
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# Καταγραφή χρόνου έναρξης για CSV
start_time_csv = time.time()

# Φόρτωση δεδομένων από CSV
crime_data_csv = spark.read.csv("data/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True, inferSchema=True)
crime_data_csv = crime_data_csv.union(spark.read.csv("data/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True, inferSchema=True))

# Επιλογή των απαραίτητων στηλών
crime_data_csv = crime_data_csv.select(
    col("DATE OCC").alias("date"),
    col("AREA NAME").alias("precinct"),
    col("Status").alias("status")
)

# Καθαρισμός τιμών στη στήλη Status
crime_data_csv = crime_data_csv.withColumn("status", trim(upper(col("status"))))

# Εξαγωγή του έτους από την ημερομηνία
crime_data_csv = crime_data_csv.withColumn("year", col("date").substr(7, 4).cast("int"))

# Καταγραφή χρόνου λήξης για CSV
end_time_csv = time.time()
print(f"Execution Time (CSV Loading): {end_time_csv - start_time_csv:.2f} seconds")

# Αποθήκευση των δεδομένων σε Parquet
crime_data_csv.write.parquet("data/output/crime_data.parquet", mode="overwrite")

# Καταγραφή χρόνου έναρξης για Parquet
start_time_parquet = time.time()

# Φόρτωση των δεδομένων από Parquet
crime_data_parquet = spark.read.parquet("data/output/crime_data.parquet")

# Καταγραφή χρόνου λήξης για Parquet
end_time_parquet = time.time()
print(f"Execution Time (Parquet Loading): {end_time_parquet - start_time_parquet:.2f} seconds")

# Υλοποίηση με SQL API
crime_data_parquet.createOrReplaceTempView("crime_data")

query = """
    WITH ranked_crimes AS (
        SELECT 
            year, 
            precinct, 
            ROUND((SUM(CASE WHEN status IN ('AA', 'JA') THEN 1 ELSE 0 END) * 100.0) / COUNT(status), 5) AS closed_case_rate,
            RANK() OVER (PARTITION BY year ORDER BY (SUM(CASE WHEN status IN ('AA', 'JA') THEN 1 ELSE 0 END) * 100.0) / COUNT(status) DESC) AS ranking
        FROM crime_data
        GROUP BY year, precinct
    )
    SELECT * FROM ranked_crimes WHERE ranking <= 3 ORDER BY year, ranking
"""

# Καταγραφή χρόνου έναρξης για SQL Query σε Parquet
start_time_query_parquet = time.time()
sql_result_parquet = spark.sql(query)
sql_result_parquet.show(truncate=False, n=sql_result_parquet.count())
end_time_query_parquet = time.time()
print(f"Execution Time (Parquet Query): {end_time_query_parquet - start_time_query_parquet:.2f} seconds")

# Καταγραφή χρόνου έναρξης για SQL Query σε CSV
crime_data_csv.createOrReplaceTempView("crime_data")
start_time_query_csv = time.time()
sql_result_csv = spark.sql(query)
sql_result_csv.show(truncate=False, n=sql_result_csv.count())
end_time_query_csv = time.time()
print(f"Execution Time (CSV Query): {end_time_query_csv - start_time_query_csv:.2f} seconds")
