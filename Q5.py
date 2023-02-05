from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import math

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Query-1").getOrCreate()
hdfs_path = "hdfs://192.168.0.1:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "input/dataset.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data = data.withColumn("days", dayofmonth("tpep_pickup_datetime"))
data = data.withColumn("month", month("tpep_pickup_datetime"))
data.createOrReplaceTempView("sql_data")

starttime = time.time()
Q5 = spark.sql("""select month, days, result, row_number() over (partition by month order by result desc) as row
                from (select month, days, avg(Tip_amount/fare_amount) as result from sql_data group by month, days)""")
Q5.createOrReplaceTempView("dataset")

Q5_help = spark.sql("select month, days, result from dataset where row<6")

Q5_help.show()
stoptime = time.time()
print("time: ", stoptime-starttime)
spark.stop()