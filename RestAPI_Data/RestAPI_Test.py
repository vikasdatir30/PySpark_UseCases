import requests as req
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark= SparkSession.builder.master('local[*]').appName('RestTest').enableHiveSupport().getOrCreate()

def get_data ():
    url="https://api.coingecko.com/api/v3/exchange_rates"
    response =  req.get(url)
    if response.status_code != 200:
        if response.status_code == 404:
            return None
        else:
            raise Exception("API Hit Failed - {0}".response.text)
    return response

lst=[]

for val in get_data().json()['rates'].values():
    lst.append(val)

df = pd.DataFrame(lst)

rdd = spark.sparkContext.parallelize(lst)

schema = StructType([
    StructField("name",StringType(),True),
    StructField("type",StringType(),True),
    StructField("unit",StringType(),True),
    StructField("value",StringType(),True)
  ])

df=rdd.toDF(schema=schema)
print(df.show())
df.coalesce(1).write.format("csv").option("header", "false").mode("append").save("")




