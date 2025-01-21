from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round, rank, trim, upper
from pyspark.sql.window import Window
import time

# Δημιουργία SparkSession
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# Καταγραφή χρόνου έναρξης DataFrame API
start_time_df = time.time()

# Φόρτωση δεδομένων
crime_data_2010_2019 = spark.read.csv("data/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True, inferSchema=True)
crime_data_2020_present = spark.read.csv("data/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True, inferSchema=True)

# Ενοποίηση των δύο συνόλων δεδομένων
crime_data = crime_data_2010_2019.union(crime_data_2020_present)

# Επιλογή των απαραίτητων στηλών
crime_data = crime_data.select(
    col("DATE OCC").alias("date"),
    col("AREA NAME").alias("precinct"),
    col("Status").alias("status")
)

# Καθαρισμός τιμών στη στήλη Status
crime_data = crime_data.withColumn("status", trim(upper(col("status"))))

# Εξαγωγή του έτους από την ημερομηνία
crime_data = crime_data.withColumn("year", col("date").substr(7, 4).cast("int"))

# Υπολογισμός συνολικών και κλεισμένων υποθέσεων
closed_cases = crime_data.groupBy("year", "precinct").agg(
    count("status").alias("total_cases"),
    count(when(col("status").isin("AA", "JA"), True)).alias("closed_cases")
)

# Υπολογισμός ποσοστού κλεισμένων υποθέσεων με έλεγχο για διαίρεση με το μηδέν
closed_cases = closed_cases.withColumn(
    "closed_case_rate", 
    when(col("total_cases") > 0, round((col("closed_cases") / col("total_cases")) * 100, 5)).otherwise(0)
)

# Ορισμός παραθύρου για ταξινόμηση ανά έτος
window_spec = Window.partitionBy("year").orderBy(col("closed_case_rate").desc())
closed_cases_ranked = closed_cases.withColumn("ranking", rank().over(window_spec))

# Επιλογή των 3 πρώτων κάθε έτους
top_3_closed_cases = closed_cases_ranked.filter(col("ranking") <= 3)

# Ταξινόμηση σύμφωνα με τις προδιαγραφές
final_result = top_3_closed_cases.select("year", "precinct", "closed_case_rate", "ranking").orderBy("year", "ranking")

# Αποθήκευση των αποτελεσμάτων με επιλογή overwrite
final_result.write.csv("data/output/top_3_closed_cases_per_year.csv", header=True, mode="overwrite")

# Εμφάνιση όλων των αποτελεσμάτων
final_result.show(truncate=False, n=final_result.count())

# Καταγραφή χρόνου λήξης και εκτύπωση χρόνου εκτέλεσης DataFrame API
end_time_df = time.time()
print(f"Execution Time (DataFrame API): {end_time_df - start_time_df:.2f} seconds")

# Υλοποίηση με SQL API
crime_data.createOrReplaceTempView("crime_data")

start_time_sql = time.time()

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
sql_result = spark.sql(query)

sql_result.show(truncate=False, n=sql_result.count())

end_time_sql = time.time()
print(f"Execution Time (SQL API): {end_time_sql - start_time_sql:.2f} seconds")
