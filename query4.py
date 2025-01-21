from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
# Apache Sedona / GeoSpark imports
from sedona.register import SedonaRegistrator
from sedona.sql.types import GeometryType
from sedona.sql.functions import st_geomfromgeojson, st_point, st_contains

def main():
    # -----------------------------------------------------------------------------
    # 1. Start Spark + Sedona
    # -----------------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("SedonaGeospatialExample") \
        .getOrCreate()

    # Register Sedona (this enables SQL & UDFs)
    SedonaRegistrator.registerAll(spark)

    # -----------------------------------------------------------------------------
    # 2. Load Crime Data, Filter Out Null Island and Keep Only "Aggravated Assault"
    # -----------------------------------------------------------------------------
    crime_data_path_2010_2019 = "data/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv"
    crime_data_path_2020_present = "data/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv"

    df_crime_10_19 = spark.read.csv(crime_data_path_2010_2019, header=True, inferSchema=True)
    df_crime_20_present = spark.read.csv(crime_data_path_2020_present, header=True, inferSchema=True)

    # Union the two sets
    df_crime_all = df_crime_10_19.unionByName(df_crime_20_present, allowMissingColumns=True)

    # Remove Null Island: (LAT=0, LON=0)
    # Adjust actual column names if needed. In your sample, columns are "LAT" and "LON".
    df_crime_filtered = df_crime_all.filter(
        (col("LAT") != 0) & (col("LON") != 0)
    )

    # Keep only crimes that contain "aggravated assault" in their description (case-insensitive)
    # In your CSV, the description column might be "Crm Cd Desc".
    df_crime_filtered = df_crime_filtered.filter(
        lower(col("Crm Cd Desc")).contains("aggravated assault")
    )

    # -----------------------------------------------------------------------------
    # 3. Convert Crime LAT,LON to Sedona Geometries (Points)
    # -----------------------------------------------------------------------------
    # We’ll create a new column "geom_point" which is the geometry representation of each crime location
    df_crime_geom = df_crime_filtered.withColumn(
        "geom_point",
        st_point(col("LON"), col("LAT"))
    )
    # Register as a temporary view so we can do SQL queries with Sedona
    df_crime_geom.createOrReplaceTempView("crimes_agg_assault")

    # -----------------------------------------------------------------------------
    # 4. Load 2010 Census Blocks (GeoJSON) and Extract the "COMM" Field
    # -----------------------------------------------------------------------------
    # Path to the geojson file, e.g. "data/2010_Census_Blocks.geojson"
    # Make sure to set multiLine=True if your geojson is multiline
    # and be mindful that some large geojson might be read as multiple lines.
    df_census_raw = spark.read.format("json").option("multiLine", True).load("data/2010_Census_Blocks.geojson")

    # Typically, the schema has something like:
    #  - geometry
    #  - properties (which might contain "COMM")
    # We select "geometry" and extract the "COMM" from the nested "properties"
    df_census = df_census_raw.select(
        col("geometry").alias("geojson"),
        col("properties.COMM").alias("COMM")
    )
    
    # Convert the "geojson" to a proper geometry column
    df_census_geom = df_census.withColumn(
        "geom_polygon",
        st_geomfromgeojson(col("geojson"))
    )

    # Create or replace a temp view for Sedona SQL
    df_census_geom.createOrReplaceTempView("census_blocks")

    # -----------------------------------------------------------------------------
    # 5. Perform a Geospatial Join: Find which COMM each crime belongs to
    #    We want all crimes where the polygon "contains" the crime point
    # -----------------------------------------------------------------------------
    # We can do it in two ways:
    #   (A) DataFrame API
    #   (B) Spark SQL with Sedona

    # (A) DataFrame API example:
    df_joined = df_crime_geom.alias("c").crossJoin(df_census_geom.alias("b")) \
        .filter(st_contains(col("b.geom_polygon"), col("c.geom_point")))

    # Now we have each crime row with the matching polygon COMM
    df_joined.select(
        col("c.DR_NO"),
        col("c.Crm Cd Desc"),
        col("b.COMM").alias("Census_COMM")
    ).show(10, truncate=False)

    # (B) Or we can do the same with Sedona SQL:
    spark.sql("""
        SELECT c.DR_NO,
               c.`Crm Cd Desc`,
               b.COMM AS Census_COMM
        FROM crimes_agg_assault c
        CROSS JOIN census_blocks b
        WHERE ST_Contains(b.geom_polygon, c.geom_point)
    """).show(10, truncate=False)

    # -----------------------------------------------------------------------------
    # 6. Next Steps: e.g., Count how many "aggravated assault" crimes per COMM
    # -----------------------------------------------------------------------------
    # Using the DataFrame approach from (A):
    df_comm_count = df_joined.groupBy("b.COMM").count().orderBy(col("count").desc())
    df_comm_count.show(20, truncate=False)

    # Or using Spark SQL:
    spark.sql("""
        SELECT b.COMM AS Census_COMM,
               COUNT(*) AS AssaultCount
        FROM crimes_agg_assault c
        CROSS JOIN census_blocks b
        WHERE ST_Contains(b.geom_polygon, c.geom_point)
        GROUP BY b.COMM
        ORDER BY AssaultCount DESC
    """).show(20, truncate=False)

    # Stop the session when done
    spark.stop()

if __name__ == "__main__":
    main()
