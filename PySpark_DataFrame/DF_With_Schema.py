#Reading pipe delimited file with pyspark dataframe

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

spark = SparkSession.builder.master('local[*]').appName('DF_with_Schema').getOrCreate()

emp_schema = StructType ([StructField("EMPLOYEE_ID" ,IntegerType(), True ),
 StructField("FIRST_NAME", StringType(), True ),
 StructField("LAST_NAME", StringType(), True ),
 StructField("EMAIL" ,StringType(), True ) ,
 StructField("PHONE_NUMBER", StringType(), True ),
 StructField("HIRE_DATE" ,DateType(), True ) ,
 StructField("JOB_ID" ,StringType(), True ) ,
 StructField("SALARY" ,FloatType(), True ) ,
 StructField("COMMISSION_PCT", FloatType(), True ),
 StructField("MANAGER_ID" ,StringType(), True ),
 StructField("DEPARTMENT_ID", FloatType(), True)])


dept_schema = StructType ([
 StructField("DEPARTMENT_ID",IntegerType(),True),
 StructField("DEPARTMENT_NAME",StringType(),True),
 StructField("MANAGER_ID",IntegerType(),True),
 StructField("LOCATION_ID",IntegerType(),True)

])


emp_df = spark.read.csv("E:\\SrcData\\EMPLOYEES.txt", schema=emp_schema, encoding='utf-8', sep='|',header=True,quote='"')
dept_df = spark.read.csv("E:\\SrcData\\Dept.txt", schema=dept_schema, encoding='utf-8', sep='|',header=True,quote='"')
emp_df.printSchema()
dept_df.printSchema()

emp_df.show(1000,truncate=False)
dept_df.show(1000,truncate=False)