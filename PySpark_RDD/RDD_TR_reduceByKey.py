#reduceByKey transformation

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('ReduceByKey').enableHiveSupport().getOrCreate()

lst = ["Hello Good Morning","Hello How are you ?","Hello What are you doing ?"]

rdd_raw = spark.sparkContext.parallelize(lst)

rdd_flt =  rdd_raw.flatMap(lambda x : x.split(" "))

rdd_map = rdd_flt.map(lambda w : (w,1))

rdd_red = rdd_map.reduceByKey(lambda w,c :w+c).collect()

for i  in rdd_red:
    print(i)

spark.stop()