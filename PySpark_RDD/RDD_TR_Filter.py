#filter transformation

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('Filter').enableHiveSupport().getOrCreate()

lst =range(1,1000000)

rdd_raw = spark.sparkContext.parallelize(lst)

rdd_even = rdd_raw.filter(lambda n : n%2==0)

print(rdd_even.collect())

spark.stop()