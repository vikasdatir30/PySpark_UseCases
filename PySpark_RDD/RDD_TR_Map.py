#Map transformation

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('Map').enableHiveSupport().getOrCreate()

lst = ["Hello, Good Morning","How are you ?","What are you doing"]

rdd_raw = spark.sparkContext.parallelize(lst)

rdd_word= rdd_raw.map(lambda x : x.split(" "))

print(rdd_word.collect())

