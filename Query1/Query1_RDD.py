# Εισαγωγή απαραίτητων βιβλιοθηκών
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

def determine_age_group(age_str):
    """
    Κατηγοριοποίηση θυμάτων με βάση την ηλικία.
    Επιστρέφει την κατηγορία:
    - 'Children' για ηλικίες < 18
    - 'Young Adults' για ηλικίες 18 - 24
    - 'Adults' για ηλικίες 25 - 64
    - 'Elderly' για ηλικίες > 64
    Αν η ηλικία δεν είναι έγκυρη, επιστρέφει 'Elderly'.
    """
    try:
        age = int(age_str)
        if age < 18:
            return "Children"
        elif 18 <= age <= 24:
            return "Young Adults"
        elif 25 <= age <= 64:
            return "Adults"
        else:
            return "Elderly"
    except:
        return "Elderly"  # Default σε περίπτωση μη έγκυρης ηλικίας

# ============== ΑΡΧΗ: Μέτρηση χρόνου ==============
start_time_rdd = time.time()
# ================================================

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("Crime Data Processing - RDD API") \
    .getOrCreate()

# Ορισμός schema για τα δεδομένα εγκλημάτων
crimes_schema = StructType([
    StructField("dr_no", StringType(), True),           # Μοναδικός αριθμός περιστατικού
    StructField("date_rptd", StringType(), True),       # Ημερομηνία αναφοράς
    StructField("date_occ", StringType(), True),        # Ημερομηνία περιστατικού
    StructField("time_occ", StringType(), True),        # Ώρα περιστατικού
    StructField("area", StringType(), True),            # Περιοχή LAPD (κωδικός)
    StructField("area_name", StringType(), True),       # Όνομα περιοχής LAPD
    StructField("rpt_dist_no", StringType(), True),     # Κωδικός περιοχής αναφοράς
    StructField("part_1_2", IntegerType(), True),       # Κατηγορία εγκλήματος (Part 1 ή 2)
    StructField("crm_cd", StringType(), True),          # Κωδικός εγκλήματος
    StructField("crm_cd_desc", StringType(), True),     # Περιγραφή εγκλήματος
    StructField("mocodes", StringType(), True),         # Τεχνικές υπόπτου
    StructField("vict_age", StringType(), True),        # Ηλικία θύματος
    StructField("vict_sex", StringType(), True),        # Φύλο θύματος
    StructField("vict_descent", StringType(), True),    # Καταγωγή θύματος
    StructField("premis_cd", StringType(), True),       # Κωδικός τοποθεσίας
    StructField("premis_desc", StringType(), True),     # Περιγραφή τοποθεσίας
    StructField("weapon_used_cd", StringType(), True),  # Κωδικός όπλου
    StructField("weapon_desc", StringType(), True),     # Περιγραφή όπλου
    StructField("status", StringType(), True),          # Κατάσταση υπόθεσης
    StructField("status_desc", StringType(), True),     # Περιγραφή κατάστασης
    StructField("crm_cd_1", StringType(), True),        # Κύριος κωδικός εγκλήματος
    StructField("crm_cd_2", StringType(), True),        # Δευτερεύων κωδικός εγκλήματος
    StructField("crm_cd_3", StringType(), True),        # Τριτεύων κωδικός εγκλήματος
    StructField("crm_cd_4", StringType(), True),        # Τεταρτεύων κωδικός εγκλήματος
    StructField("location", StringType(), True),        # Τοποθεσία περιστατικού
    StructField("cross_street", StringType(), True),    # Διασταύρωση
    StructField("lat", FloatType(), True),              # Γεωγραφικό πλάτος
    StructField("lon", FloatType(), True)               # Γεωγραφικό μήκος
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

# Μετατροπή DataFrame σε RDD
crimes_rdd = crimes_df.rdd

# Φιλτράρισμα περιστατικών που περιέχουν "AGGRAVATED ASSAULT" 
aggr_assault_rdd = crimes_rdd.filter(lambda row: 
    row.crm_cd_desc is not None and "AGGRAVATED ASSAULT" in row.crm_cd_desc.upper()
)

# Δημιουργία RDD (age_group, 1)
age_groups_rdd = aggr_assault_rdd.map(lambda row: (determine_age_group(row.vict_age), 1))

# Συγκέντρωση δεδομένων με reduceByKey
counts_rdd = age_groups_rdd.reduceByKey(lambda x, y: x + y)

# Ταξινόμηση αποτελεσμάτων σε φθίνουσα σειρά
sorted_rdd = counts_rdd.sortBy(lambda x: x[1], ascending=False)

# Συλλογή αποτελεσμάτων 
results = sorted_rdd.collect()

# Προβολή αποτελεσμάτων
print("Age Group | Count")
for (age_grp, cnt) in results:
    print(f"{age_grp}\t{cnt}")

# ============== ΤΕΛΟΣ: Μέτρηση χρόνου ==============
end_time_rdd = time.time()
elapsed_rdd = end_time_rdd - start_time_rdd
print(f"RDD API elapsed time: {elapsed_rdd:.2f} seconds.")
# ================================================

