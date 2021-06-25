import requests as req
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import lit
from datetime import datetime as dt



spark= SparkSession.builder.master('local[2]')\
    .appName('RestTest').getOrCreate()

def get_data ():
    url="https://api.coingecko.com/api/v3/exchange_rates"
    response =  req.get(url)
    if response.status_code != 200:
        if response.status_code == 404:
            return None
        else:
            raise Exception("API Hit Failed - {0}".response.text)
    print(response.text)
    return response



def load():
    lst=[]
    schema_flt = StructType([
                        StructField("name", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("unit", StringType(), True),
                        StructField("value", DoubleType(), True),
                        StructField("code",StringType(),True),
                        StructField("Time", StringType(),False)
    ])

    schema = spark.read.json('E:\\SrcData\\rates_data.json').schema
    raw_df=spark.read.json('E:\\SrcData\\rates_data.json',schema=schema, encoding='utf8')

    #flt_df= spark.createDataFrame(spark.sparkContext.parallelize([]),schema=raw_df.select("rates.aed.*").schema)
    flt_df = spark.createDataFrame(spark.sparkContext.parallelize([]), schema=schema_flt)

    #print(raw_df.select("rates.*").columns)

    stmp=dt.now().strftime("%Y-%m-%d %H:%M")
    #selecting required cols from raw data frame
    for col in raw_df.select("rates.*").columns:
        flt_df=flt_df.union(raw_df.select("rates."+col+".*").\
                            withColumn("code",lit(col)).\
                            withColumn("time",lit(stmp)))

    flt_df.coalesce(1).write.format("jdbc").\
        option("url", "jdbc:oracle:thin:hr/admin@//192.168.1.100:1521/xe").\
        option("user", "hr").\
        option("password", "admin").\
        option("dbtable","rates").\
        option("driver", "oracle.jdbc.driver.OracleDriver").\
        mode("append").save()

load()