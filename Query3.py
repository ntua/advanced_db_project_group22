# Εισαγωγή απαραίτητων βιβλιοθηκών
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, when, expr,
    sum as spark_sum, count
)
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType

# Sedona Imports
from sedona.spark import SedonaContext
from sedona.register import SedonaRegistrator

# Δημιουργία SparkSession + Sedona
spark = (
    SparkSession.builder
    .appName("Query3-Full-Code")
    .config("spark.jars", "/jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar,/jars/geotools-wrapper-1.6.1-28.2.jar")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .getOrCreate()
)

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
    "data/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv",
    header=True,
    schema=crimes_schema
)
crimes_df2 = spark.read.csv(
    "data/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv",
    header=True,
    schema=crimes_schema
)

# Ενοποίηση των δύο DataFrames
crimes_df = crimes_df1.union(crimes_df2)

# Φιλτράρουμε null συντεταγμένες
crimes_df = crimes_df.filter(
    (col("LAT").isNotNull()) &
    (col("LON").isNotNull())
)

# Προσθέτουμε γεωμετρική στήλη ST_Point
crimes_geom_df = crimes_df.withColumn(
    "geom",
    expr("ST_Point(LON, LAT)")
)

# Φόρτωση 2010_Census_Blocks.geojson
# Φιλτράρουμε CITY='Los Angeles'
geojson_path = "data/2010_Census_Blocks.geojson"

blocks_raw_df = (spark.read.format("geojson")
    .option("multiLine","true")
    .load(geojson_path)
    .selectExpr("explode(features) as features")
    .select("features.*")
)

census_df = blocks_raw_df.select(
    col("properties.ZCTA10").alias("ZCTA10"),
    col("properties.COMM").alias("COMM"),
    col("properties.POP_2010").alias("POP_2010"),
    col("properties.CITY").alias("CITY"),
    col("geometry").alias("geom_polygon")
)

census_df = census_df.filter(
    (col("CITY") == "Los Angeles") &
    (col("ZCTA10").isNotNull()) &
    (col("POP_2010") > 0)
)

# Join γεωχωρικό (ST_Within)
joined_spatial_df = crimes_geom_df.join(
    census_df,
    expr("ST_Within(geom, geom_polygon)"),
    "left"
)

# Ομαδοποίηση εγκλημάτων ανά ZCTA10
crime_by_zip_df = joined_spatial_df.groupBy("ZCTA10").agg(
    count("*").alias("crime_count")
)

# Πληθυσμός ανά (ZCTA10, COMM)
pop_by_zip_comm = census_df.groupBy("ZCTA10","COMM").agg(
    spark_sum("POP_2010").alias("pop_2010")
)

# Φόρτωση εισοδήματος (CSV)
income_schema = """
Zip_Code STRING,
Community STRING,
Estimated_Median_Income STRING
"""

income_df = (spark.read
    .option("header",True)
    .schema(income_schema)
    .csv("data/LA_income_2015.csv")
)

income_df = income_df.withColumn(
    "Estimated_Median_Income",
    regexp_replace(col("Estimated_Median_Income"), "[$,]", "").cast("float")
)

# Δοκιμή 4 στρατηγικών JOIN σε
# pop_by_zip_comm.join(crime_by_zip_df)
# Φτιάχνουμε 4 DataFrames, το καθένα με άλλο hint,
# κατόπιν .explain(), .show(), κ.λπ.
from pyspark.sql.functions import lit

join_conditions = (pop_by_zip_comm.ZCTA10 == crime_by_zip_df.ZCTA10)
join_type = "left"

hints = ["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]

for strategy in hints:
    print(f"=== TEST JOIN with hint({strategy}) ===")
    
    # ============== ΑΡΧΗ: Μέτρηση χρόνου ==============
    start_time = time.time()
    # ================================================

    # Κάνουμε join με το συγκεκριμένο hint
    df_test_join = pop_by_zip_comm.join(
        crime_by_zip_df.hint(strategy),
        join_conditions,
        join_type
    ).select(
        pop_by_zip_comm["COMM"],
        pop_by_zip_comm["ZCTA10"],
        pop_by_zip_comm["pop_2010"],
        crime_by_zip_df["crime_count"]
    )

    # .explain() για να δεις το σχέδιο
    df_test_join.explain()

    # Εκτέλεση π.χ. .count() ή .show() για να μετρήσουμε χρόνο
    df_test_join_count = df_test_join.count()
    print(f"Join DF row count: {df_test_join_count}")

    # ============== ΤΕΛΟΣ: Μέτρηση χρόνου ==============
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"[{strategy} Join] Elapsed time: {elapsed:.2f} secs\n")
    # ================================================

# Τώρα επιλέγουμε π.χ. BROADCAST ή MERGE (ό,τι μας συμφέρει)
# ας πούμε pop_crime_df = df_test_join χωρίς hint κλπ.

pop_crime_df = pop_by_zip_comm.join(
    crime_by_zip_df.hint("BROADCAST"),
    join_conditions,
    join_type
).select(
    pop_by_zip_comm["COMM"],
    pop_by_zip_comm["ZCTA10"],
    pop_by_zip_comm["pop_2010"],
    crime_by_zip_df["crime_count"]
)

# Συνδυασμός με εισοδήματα
pop_crime_income_df = pop_crime_df.join(
    income_df, pop_crime_df.ZCTA10 == income_df.Zip_Code, "left"
).select(
    pop_crime_df["COMM"],
    pop_crime_df["ZCTA10"],
    pop_crime_df["pop_2010"],
    pop_crime_df["crime_count"],
    income_df["Estimated_Median_Income"]
)

# Ομαδοποίηση τελική ανά COMM
final_comm_df = pop_crime_income_df.groupBy("COMM").agg(
    spark_sum("pop_2010").alias("total_population"),
    spark_sum("crime_count").alias("total_crimes"),
    spark_sum(col("Estimated_Median_Income") * col("pop_2010")).alias("income_pop_sum")
)

final_comm_df = final_comm_df.withColumn(
    "income_per_person",
    when(col("total_population") > 0, col("income_pop_sum") / col("total_population"))
).withColumn(
    "crimes_per_person",
    when(col("total_population") > 0, col("total_crimes") / col("total_population"))
)

# Προβολή Τελικού Αποτελέσματος
final_comm_df.show(50, truncate=False)

spark.stop()
