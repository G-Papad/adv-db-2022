# Προχωρημένα Θέματα Βάσεων Δεδομένων - Εξαμηνιαία Εργασία
Γεώργιος Παπαδούλης - Χριστίνα Προεστάκη

## Εγκατάσταση Spark σύμφωνα με τις οδηγίες του εργαστηρίου
1. Install python3.8
2. Install pip
3. Install PySpark
4. Install Apache Spark
5. Install Java
6. Setup a Cluster (1 master and 1 worker)

Deployment of workers:
spark-daemon.sh start org.apache.spark.deploy.worker.Worker {worker-id} --webui-port 8080 --port 65509 --cores 4 --memory 8g spark://{master-local-ip} 
όπου master-local-ip σε εμάς: 192.168.0.1

## Εγκατάσταση hdfs
Οδηγίες σύμφωνα με το παρακάτω link: https://sparkbyexamples.com/hadoop/apache-hadoop-installation/

## Προεπεξεργασία Δεδομένων
1. Κατεβάζουμε τα δεδομένα από https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Eκτελούμε το make_db.py script που κάνει concat τα παραπάνω αρχεία δεδομένων σε ένα
3. Δημιουργείται ένας φάκελος και μετανομάζουμε το .parquet αρχείο σε dataset.parquet
4. Δημιουργούμε ένα φάκελο στο hadoop με hdfs dfs -mkdir /input
5. Ανεβάζουμε το αρχείο στο hadoop με την εντολή hdfs dfs -put <filename> /input

## Δημιουργία Dataframes
hdfs_path = "hdfs://192.168.0.1:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "input/dataset.parquet")

## Δημιουργία RDD
Για τα dataframes που έχουν εισαχθεί από τις παραπάνω εντολές:   
dataset_rdd = data.rdd
