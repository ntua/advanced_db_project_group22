# advanced_db_project_group22
# Spark & Sedona Setup for Query 3 & Query 5

Αυτό το repository περιέχει κώδικα σε **PySpark** + **[Apache Sedona](https://sedona.apache.org/)** για την εκτέλεση των queries:

1. **Query 1** – Ταξινόμηση ηλικιακών ομάδων θυμάτων σε περιστατικά "βαριάς σωματικής βλάβης" σε φθίνουσα σειρά.  
2. **Query 2** – Εύρεση των 3 Αστυνομικών Τμημάτων με τα υψηλότερα ποσοστά κλεισμένων υποθέσεων ανά έτος και κατάταξή τους.  
3. **Query 3** – Υπολογισμός Μέσου Ετήσιου Εισοδήματος και Αναλογίας Εγκλημάτων ανά Άτομο για κάθε περιοχή του Los Angeles.  
4. **Query 4** – Εξέταση φυλετικού προφίλ θυμάτων εγκλημάτων στις περιοχές με το υψηλότερο και χαμηλότερο κατά κεφαλήν εισόδημα το 2015.  
5. **Query 5** – Υπολογισμός αριθμού εγκλημάτων και μέσης απόστασης από κάθε Αστυνομικό Τμήμα.


## Προαπαιτούμενα

- **Ubuntu** ή παρόμοιο Linux λειτουργικό.  
- **Java/OpenJDK** (π.χ. 11).  
- **Python 3 + pip**.  
- **Apache Spark** (εκδόσεις 3.0+ συνιστώνται).  
- **Apache Sedona** jars (sedona-spark-shaded & geotools-wrapper).  
- Python βιβλιοθήκες: `apache-sedona==1.6.1`, `geopandas`, `shapely`, κ.ά.

## Εκτέλεση του Setup Script

1. Κλωνοποίηστε το repo (ή κατέβαστέ το).  
2. Μέσα στον φάκελο, θα βρειτε ένα script `install-dependencies.sh`. Δώστε:

   ```bash
   chmod +x install-dependencies.sh
   sudo ./install-dependencies.sh
3. Αφού εγκαταστήσετε τις εξαρτήσεις, μπορείτε να εκτελέσετε τον κώδικα Python χρησιμοποιώντας τον εξής τρόπο:
   
   python3 <name.py>
