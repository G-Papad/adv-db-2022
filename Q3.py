from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import math

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Query-3").getOrCreate()
hdfs_path = "hdfs://192.168.0.1:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "input/dataset.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

data = data.withColumn("dekapenthimero", 2*(month("tpep_pickup_datetime")-1) + floor(dayofmonth("tpep_pickup_datetime")/16))
data.createOrReplaceTempView("sql_data")

starttime = time.time()
Q3 = spark.sql("Select dekapenthimero, avg(Trip_distance), avg(total_amount) from sql_data where PULocationID != DOLocationID group by dekapenthimero ")
Q3.show()
stoptime = time.time()
print("DF-query time: ", stoptime-starttime)

#Map-Reduce

dataset_rdd = data.rdd
rdd_starttime = time.time()
Q3_rdd = dataset_rdd.filter(lambda x: x.PULocationID != x.DOLocationID)\
          .map(lambda x: (str(x.dekapenthimero), (float(x.trip_distance),1, float(x.total_amount))))\
          .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))\
          .mapValues(lambda x: ((x[0]/x[1])*100, (x[2]/x[1])))

for x in Q3_rdd.collect():
        print(x)

rdd_stoptime = time.time()
print("RDD-query time: ", rdd_stoptime - rdd_starttime)

spark.stop()