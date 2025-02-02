# Εισαγωγή απαραίτητων βιβλιοθηκών
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, when

# ============== ΑΡΧΗ: Μέτρηση χρόνου ==============
start_time_df = time.time()
# ================================================

# Δημιουργία SparkSession
spark = SparkSession \
    .builder \
    .appName("Crime Data Processing") \
    .getOrCreate()

# Ορισμός schema για τα δεδομένα εγκλημάτων
crimes_schema = StructType([
    StructField("dr_no", StringType(), True),           # Μοναδικός αριθμός για κάθε περιστατικό
    StructField("date_rptd", StringType(), True),       # Ημερομηνία αναφοράς του περιστατικού
    StructField("date_occ", StringType(), True),        # Ημερομηνία που συνέβη το περιστατικό
    StructField("time_occ", StringType(), True),        # Ώρα που συνέβη το περιστατικό
    StructField("area", StringType(), True),            # Κωδικός γεωγραφικής περιοχής LAPD
    StructField("area_name", StringType(), True),       # Όνομα γεωγραφικής περιοχής LAPD
    StructField("rpt_dist_no", StringType(), True),     # Κωδικός περιοχής αναφοράς
    StructField("part_1_2", IntegerType(), True),       # Κατηγορία εγκλήματος (Part 1 ή 2)
    StructField("crm_cd", StringType(), True),          # Κωδικός εγκλήματος
    StructField("crm_cd_desc", StringType(), True),     # Περιγραφή του εγκλήματος
    StructField("mocodes", StringType(), True),         # Τεχνικές που χρησιμοποίησε ο δράστης
    StructField("vict_age", StringType(), True),        # Ηλικία του θύματος
    StructField("vict_sex", StringType(), True),        # Φύλο του θύματος
    StructField("vict_descent", StringType(), True),    # Καταγωγή του θύματος
    StructField("premis_cd", StringType(), True),       # Κωδικός χώρου που συνέβη το περιστατικό
    StructField("premis_desc", StringType(), True),     # Περιγραφή χώρου που συνέβη το περιστατικό
    StructField("weapon_used_cd", StringType(), True),  # Κωδικός όπλου που χρησιμοποιήθηκε
    StructField("weapon_desc", StringType(), True),     # Περιγραφή του όπλου
    StructField("status", StringType(), True),          # Κατάσταση υπόθεσης
    StructField("status_desc", StringType(), True),     # Περιγραφή κατάστασης
    StructField("crm_cd_1", StringType(), True),        # Κύριος κωδικός εγκλήματος
    StructField("crm_cd_2", StringType(), True),        # Δευτερεύων κωδικός εγκλήματος
    StructField("crm_cd_3", StringType(), True),        # Τριτεύων κωδικός εγκλήματος
    StructField("crm_cd_4", StringType(), True),        # Τεταρτεύων κωδικός εγκλήματος
    StructField("location", StringType(), True),        # Τοποθεσία που συνέβη το περιστατικό
    StructField("cross_street", StringType(), True),    # Διασταύρωση κοντά στην τοποθεσία
    StructField("lat", FloatType(), True),              # Γεωγραφικό πλάτος
    StructField("lon", FloatType(), True)               # Γεωγραφικό μήκος
])

# Φόρτωση δεδομένων εγκλημάτων 
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
# Συνένωση των δύο DataFrames
crimes_df = crimes_df1.union(crimes_df2)

# Φιλτράρισμα περιστατικών που περιλαμβάνουν τον όρο "AGGRAVATED ASSAULT"
aggravated_assault_df = crimes_df.filter(crimes_df["crm_cd_desc"].contains("AGGRAVATED ASSAULT"))

# Δημιουργία νέας στήλης για ηλικιακές ομάδες
aggravated_assault_df = aggravated_assault_df.withColumn(
    "age_group",
    when(col("vict_age").cast("int") < 18, "Children")
    .when((col("vict_age").cast("int") >= 18) & (col("vict_age").cast("int") <= 24), "Young Adults")
    .when((col("vict_age").cast("int") >= 25) & (col("vict_age").cast("int") <= 64), "Adults")
    .otherwise("Elderly")
)

# Ομαδοποίηση και καταμέτρηση περιστατικών ανά ηλικιακή ομάδα
age_group_counts_df = aggravated_assault_df.groupBy("age_group").count()

# Ταξινόμηση των αποτελεσμάτων σε φθίνουσα σειρά με βάση την καταμέτρηση
age_group_counts_sorted_df = age_group_counts_df.orderBy(col("count").desc())

# Προβολή αποτελεσμάτων
age_group_counts_sorted_df.show()

# ============== ΤΕΛΟΣ: Μέτρηση χρόνου ==============
end_time_df = time.time()
elapsed_df = end_time_df - start_time_df
print(f"Elapsed time for DataFrame API execution: {elapsed_df:.2f} seconds.")
# ================================================