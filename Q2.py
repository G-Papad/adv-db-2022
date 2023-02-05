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

starttime= time.time()
#Qtemp = spark.sql("Select month, max(tolls_amount) from sql_data group by month")
#Qtemp.show()
Q2 = spark.sql("Select * from (select month, max(tolls_amount) as tolls from sql_data group by month) as X inner join sql_data on X.tolls=sql_data.tolls_amount")
Q2.show(10)
stoptime = time.time()
print("time: ", stoptime - starttime)
spark.stop()
