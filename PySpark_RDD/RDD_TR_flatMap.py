#flatMap transformation

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('Test').enableHiveSupport().getOrCreate()

lst = ["Hello, Good Morning","How are you ?","What are you doing"]

rdd_raw = spark.sparkContext.parallelize(lst)

rdd_word= rdd_raw.flatMap(lambda x : x.split(" "))


print(rdd_word.collect())

