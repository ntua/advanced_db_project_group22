import sys
if sys.version_info >= (3, 11):
    sys.exit("Error: Apache Sedona 1.6.1 is not compatible with Python 3.11. Please use Python 3.10.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, desc
from sedona.spark import SedonaContext

def main():
    # -----------------------------------------------------------------------------
    # 1. Start Spark Session with Sedona Configurations
    # -----------------------------------------------------------------------------
    spark = (SparkSession.builder
        .appName("Query4_RacialProfile_CrimeVictims_LA") 
        .config("spark.executor.instances", "8")  
        .config("spark.executor.cores", "1")  
        .config("spark.executor.memory", "2g")  
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memoryOverhead", "2g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    
    # Initialize Sedona Context (requires a supported Python version)
    sedona = SedonaContext.create(spark)

    # -----------------------------------------------------------------------------
    # 2. Load and Prepare Crime Data for 2015
    # -----------------------------------------------------------------------------
    crime_data_2015_path = "data/CrimeData/Crime_Data_2015.csv"
    df_crime_2015 = spark.read.csv(crime_data_2015_path, header=True, inferSchema=True)
    df_crime_2015 = df_crime_2015.withColumn("Year", col("Date").substr(-4, 4).cast("int")) \
                                 .filter(col("Year") == 2015)

    # -----------------------------------------------------------------------------
    # 3. Load Income Data and Identify Top 3 and Bottom 3 Areas
    # -----------------------------------------------------------------------------
    income_data_path = "data/LA_income_2015.csv"
    df_income = spark.read.csv(income_data_path, header=True, inferSchema=True)
    df_income = df_income.withColumn("Income", lower(col("Estimated Median Income")).replace({"\\$|,": ""}, regex=True).cast("int")) \
                         .withColumnRenamed("Zip Code", "ZipCode")
    top_3_income_zips = df_income.orderBy(desc("Income")).limit(3).select("ZipCode").rdd.flatMap(lambda x: x).collect()
    bottom_3_income_zips = df_income.orderBy("Income").limit(3).select("ZipCode").rdd.flatMap(lambda x: x).collect()

    # -----------------------------------------------------------------------------
    # 4. Load Race and Ethnicity Codes and Map Descent Codes
    # -----------------------------------------------------------------------------
    race_ethnicity_codes_path = "data/RE_codes.csv"
    df_re_codes = spark.read.csv(race_ethnicity_codes_path, header=True, inferSchema=True) \
                           .withColumnRenamed("Vict Descent", "DescentCode") \
                           .withColumnRenamed("Vict Descent Full", "DescentFull")
    from pyspark.sql.functions import broadcast
    df_re_codes_broadcast = broadcast(df_re_codes)
    df_crime_mapped = df_crime_2015.join(df_re_codes_broadcast, on="DescentCode", how="left") \
                                  .withColumnRenamed("DescentFull", "Race_Final")

    # -----------------------------------------------------------------------------
    # 5. Filter Crime Data for Top 3 and Bottom 3 Income Areas
    # -----------------------------------------------------------------------------
    df_crime_top3 = df_crime_mapped.filter(col("ZipCode").isin(top_3_income_zips))
    df_crime_bottom3 = df_crime_mapped.filter(col("ZipCode").isin(bottom_3_income_zips))

    # -----------------------------------------------------------------------------
    # 6. Aggregate Racial Profiles
    # -----------------------------------------------------------------------------
    df_racial_profile_top3 = df_crime_top3.groupBy("Race_Final").count().orderBy(desc("count"))
    print("=== Racial Profile of Crime Victims in Top 3 Highest-Income Areas (2015) ===")
    df_racial_profile_top3.show(truncate=False)

    df_racial_profile_bottom3 = df_crime_bottom3.groupBy("Race_Final").count().orderBy(desc("count"))
    print("=== Racial Profile of Crime Victims in Bottom 3 Lowest-Income Areas (2015) ===")
    df_racial_profile_bottom3.show(truncate=False)

    # -----------------------------------------------------------------------------
    # 7. Stop the Spark Session
    # -----------------------------------------------------------------------------
    spark.stop()

if __name__ == "__main__":
    main()
