from pyspark.sql import SparkSession
spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Db concat all files").getOrCreate()
df = spark.read.parquet("yellow_tripdata_2022-01.parquet")
for i in range(2, 7):
        df = df.union(spark.read.parquet("yellow_tripdata_2022-0{}.parquet".format(i)))
df.coalesce(1).write.format("parquet").mode("append").save("dataset")
spark.stop()