from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Query-1").getOrCreate()
hdfs_path = "hdfs://192.168.0.1:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "input/dataset.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data2  = data.withColumn("month", month("tpep_pickup_datetime"))
data2.createOrReplaceTempView("sql_data")

zone_lookups = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path + "input/taxi+_zone_lookup.csv")
zone_lookups.createOrReplaceTempView("zone_lookup")
starttime = time.time()
#Q1palio = spark.sql("select * from sql_data where month == 3 and DOLocationID == 12 order by tip_amount desc limit 1")
#Q1palio.show()
Q1 = spark.sql("select * from (select Zone, LocationID as id from zone_lookup where Zone='Battery Park') inner join (select * from sql_data where month == 3) as X on X.DOLocationID=id order by tip_amount desc limit 1")
Q1.show()
stoptime=time.time()
print("Q1:total time is ", stoptime-starttime)
spark.stop()