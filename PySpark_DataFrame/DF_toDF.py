#toDF() function

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[4]').appName('toDF_Test').enableHiveSupport().getOrCreate()


cols=['lanaguage', 'user_count']
data = [('Java', '30000'), ('Python','45000'),('Scala','3400')]

rdd =spark.sparkContext.parallelize(data)

df = rdd.toDF()
df.printSchema()

df_cols = rdd.toDF(cols)
df_cols.printSchema()
df_cols.show()

spark.stop()