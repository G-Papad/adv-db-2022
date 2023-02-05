from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import math

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Query-1").getOrCreate()
hdfs_path = "hdfs://192.168.0.1:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "input/dataset.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data = data.withColumn("days", dayofweek("tpep_pickup_datetime"))
data = data.withColumn("hour", hour("tpep_pickup_datetime"))
data.createOrReplaceTempView("sql_data")

starttime = time.time()
Q4 = spark.sql("""select days, hour, result, row_number() over (partition by days order by result desc) as row
                from (select days, hour, sum(Passenger_count) as result from sql_data group by days, hour)""")
Q4.createOrReplaceTempView("dataset4")

Q4_help = spark.sql("select days, hour, result from dataset4 where row<4")

Q4_help.show()
stoptime = time.time()
print("time: ", stoptime-starttime)
spark.stop()