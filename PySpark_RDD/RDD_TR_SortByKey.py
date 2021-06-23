#word count with pyspark

from  pyspark.sql  import SparkSession

spark = SparkSession.builder.master('local[1]').appName('WordCount').getOrCreate()

rdd =  spark.sparkContext.textFile('E:\\SrcData\\story.txt', use_unicode='UTF-8')

rdd_lines = rdd.map(lambda x : x.split('\n'))
print('No of lines : ', rdd_lines.count())

rdd_words = rdd.flatMap(lambda x : x.split(' '))
print('No of words : ', rdd_words.count())

#map -reduce
rdd_map= rdd_words.map(lambda x : (x,1))
rdd_reduce =  rdd_map.reduceByKey(lambda  a,b : a+b)
rdd_sort = rdd_reduce.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

print('Highest word count :', rdd_sort.take(1))
rdd_sort.coalesce(1).saveAsTextFile('E:\\TgtData\\WordCount')	











