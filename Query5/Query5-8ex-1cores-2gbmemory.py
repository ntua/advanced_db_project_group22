# Εισαγωγή απαραίτητων βιβλιοθηκών
import time
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, when, expr, row_number, desc, avg, count as spark_count
)
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType

# Sedona Imports
from sedona.spark import SedonaContext
from sedona.register import SedonaRegistrator

# ============== ΑΡΧΗ: Μέτρηση χρόνου ==============
start_time = time.time()
# ================================================
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 1) Δημιουργία SparkSession + Sedona 
spark = (
    SparkSession.builder
    .appName("Query5-Full-Code")
    .config("spark.jars", "../jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar,../jars/geotools-wrapper-1.6.1-28.2.jar")
    .config("spark.executor.instances", "8")  
    .config("spark.executor.cores", "1")  
    .config("spark.executor.memory", "2g")  
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")
    .config("spark.driver.maxResultSize", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

sedona = SedonaContext.create(spark)
SedonaRegistrator.registerAll(spark)

# Ορισμός schema για τα δεδομένα εγκλημάτων
crimes_schema = StructType([
    StructField("DR_NO", StringType(), True),
    StructField("Date Rptd", StringType(), True),
    StructField("DATE OCC", StringType(), True),
    StructField("TIME OCC", IntegerType(), True),
    StructField("AREA", StringType(), True),
    StructField("AREA NAME", StringType(), True),
    StructField("Rpt Dist No", StringType(), True),
    StructField("Part 1-2", IntegerType(), True),
    StructField("Crm Cd", StringType(), True),
    StructField("Crm Cd Desc", StringType(), True),
    StructField("Mocodes", StringType(), True),
    StructField("Vict Age", IntegerType(), True),
    StructField("Vict Sex", StringType(), True),
    StructField("Vict Descent", StringType(), True),
    StructField("Premis Cd", StringType(), True),
    StructField("Premis Desc", StringType(), True),
    StructField("Weapon Used Cd", StringType(), True),
    StructField("Weapon Desc", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Status Desc", StringType(), True),
    StructField("Crm Cd 1", StringType(), True),
    StructField("Crm Cd 2", StringType(), True),
    StructField("Crm Cd 3", StringType(), True),
    StructField("Crm Cd 4", StringType(), True),
    StructField("LOCATION", StringType(), True),
    StructField("Cross Street", StringType(), True),
    StructField("LAT", FloatType(), True),
    StructField("LON", FloatType(), True)
])

# Φόρτωση δεδομένων 
crimes_df1 = spark.read.csv(
    "../data/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv",
    header=True,
    schema=crimes_schema
)
crimes_df2 = spark.read.csv(
    "../data/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv",
    header=True,
    schema=crimes_schema
)

# Ενοποίηση των δύο DataFrames
crimes_df = crimes_df1.union(crimes_df2)

# Φιλτράρουμε null συντεταγμένες
crimes_df = crimes_df.filter(
    (col("LAT").isNotNull()) & (col("LON").isNotNull())
)

# Προσθέτουμε γεωμετρική στήλη ST_Point
crimes_df = crimes_df.withColumn(
    "geom_crime",
    expr("ST_Point(LON, LAT)")
)

# Φόρτωση LA_Police_Stations.csv
stations_schema = """
X DOUBLE,
Y DOUBLE,
FID INT,
DIVISION STRING,
LOCATION STRING,
PREC INT
"""

stations_df = (
    spark.read
    .option("header", True)
    .schema(stations_schema)
    .csv("../data/LA_Police_Stations.csv")
)

# Προσθέτουμε γεωμετρική στήλη για stations
stations_df = stations_df.withColumn(
    "geom_station",
    expr("ST_Point(X, Y)")
)

# Cross Join -> υπολογισμός απόστασης crime - station
# Επειδή τα station είναι λίγα, μπορούμε να πούμε hint("BROADCAST")
joined_df = crimes_df.crossJoin(
    stations_df.hint("BROADCAST")
).withColumn(
    "distance_m",
    expr("ST_DistanceSphere(geom_crime, geom_station)")
)

# Μετατρέπουμε σε km (προαιρετικά)
joined_df = joined_df.withColumn("distance_km", col("distance_m")/1000.0)

# Εύρεση πλησιέστερου station (Window row_number)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

win = Window.partitionBy("DR_NO").orderBy(col("distance_km").asc())

closest_df = joined_df.withColumn(
    "rank",
    row_number().over(win)
).filter(col("rank") == 1)

# Τώρα κάθε crime έχει columns: (DR_NO, DIVISION, distance_km, ...)
# Ομαδοποίηση ανά station -> count, avg(distance)
from pyspark.sql.functions import avg, count as spark_count

station_crimes_df = closest_df.groupBy("DIVISION").agg(
    spark_count("*").alias("crime_count"),
    avg("distance_km").alias("avg_distance_km")
)

# Ταξινόμηση φθίνουσα σε crime_count
result_df = station_crimes_df.orderBy(col("crime_count").desc())

# Προβολή αποτελεσμάτων
result_df.show(50, truncate=False)

# ============== ΤΕΛΟΣ: Μέτρηση χρόνου ==============
end_time = time.time()
elapsed = end_time - start_time
print(f"Query 5 completed in {elapsed:.2f} seconds.")
# ================================================

# Αν θες .explain():
#result_df.explain()

spark.stop()