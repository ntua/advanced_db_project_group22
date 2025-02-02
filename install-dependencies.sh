# 1) Ενημέρωση package lists και εγκατάσταση Java + Python 3 + pip (αν δεν υπάρχουν)
echo "Updating apt and installing Java + Python3 + pip..."
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk python3 python3-pip curl
python3 -m venv ~/spark_env
source ~/spark_env/bin/activate

# 2) Δημιουργία φακέλου /jars για τα Sedona jars
echo "Creating /jars directory for Sedona .jar files..."
sudo mkdir -p /jars

# 3) Λήψη του sedona-spark-shaded jar (Apache Sedona)
echo "Downloading sedona-spark-shaded jar (1.6.1) ..."
sudo curl -L -o /jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar \
    "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.6.1/sedona-spark-shaded-3.5_2.12-1.6.1.jar"

# 4) Λήψη του geotools-wrapper jar (GeoTools)
echo "Downloading geotools-wrapper jar ..."
sudo curl -L -o /jars/geotools-wrapper-1.6.1-28.2.jar \
    "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.6.1-28.2/geotools-wrapper-1.6.1-28.2.jar"

# 5) Εγκατάσταση Python βιβλιοθηκών μέσω pip
echo "Installing required Python libraries via pip..."
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install apache-sedona==1.6.1 shapely geopandas attrs matplotlib descartes keplergl==0.3.2 pydeck==0.8.0 pandas

# 6) Επαλήθευση λήψης jar αρχείων
echo "Verifying Sedona jar files in /jars:"
ls /jars | grep sedona || echo "sedona jar NOT FOUND"
ls /jars | grep geotools || echo "geotools jar NOT FOUND"

# 7) Επαλήθευση εγκατάστασης sedona (Python)
echo "Verifying pip install of apache-sedona..."
python3 -m pip list | grep apache-sedona || echo "apache-sedona NOT INSTALLED"

echo "Setup complete! You can now run your Spark-Sedona jobs."

