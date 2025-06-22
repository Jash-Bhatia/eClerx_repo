# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

#df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

Ways to create a schema
users_schema= string(simple) or pyspark datatype (complex)

# COMMAND ----------

emp id ,name,age,city
str_users_schema="emp_id int, name string, age int, city string"

{"id":1,"name":"a","age":34,"city":"Pune"}
str_users_schema="id int, name string, age int, city string"

{"id":1,"name":"a","age":34,"city":"Pune",mobile:[1234,67898]}
str_users_schema="id int, name string, age int, city string, mobile list/array" (FAIL)

{"id":1,"name":"a","age":34,"city":"Pune",mobile:{"home":234,"office":5678}}


special datatypes in pyspark

Struct Type
Array type
map type

# COMMAND ----------

id,name,age,city

StructType([StructField("id",IntegerType()),
            StructField("name",StringType()),
            StructField("age",IntegerType()),
            StructField("city",StringType()),
            StructField("mobile",ArrayType())

])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DepTime", IntegerType(), True),
    StructField("CRSDepTime", IntegerType(), True),
    StructField("ArrTime", IntegerType(), True),
    StructField("CRSArrTime", IntegerType(), True),
    StructField("UniqueCarrier", StringType(), True),
    StructField("FlightNum", IntegerType(), True),
    StructField("TailNum", StringType(), True),
    StructField("ActualElapsedTime", IntegerType(), True),
    StructField("CRSElapsedTime", IntegerType(), True),
    StructField("AirTime", IntegerType(), True),
    StructField("ArrDelay", IntegerType(), True),
    StructField("DepDelay", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", IntegerType(), True),
    StructField("TaxiIn", IntegerType(), True),
    StructField("TaxiOut", IntegerType(), True),
    StructField("Cancelled", IntegerType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", IntegerType(), True),
    StructField("CarrierDelay", IntegerType(), True),
    StructField("WeatherDelay", IntegerType(), True),
    StructField("NASDelay", IntegerType(), True),
    StructField("SecurityDelay", IntegerType(), True),
    StructField("LateAircraftDelay", StringType(), True)
])

df = spark.read.csv("dbfs:/databricks-datasets/asa/airlines/", header=True, schema=schema)

# COMMAND ----------

df_2007=df.filter("Year=2007")

# COMMAND ----------

df_2007_month=df_2007.groupBy("month").count()

# COMMAND ----------

df_2007_month.display()

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

Transformations:

    Narrow
    filter

    Wide(shuffle tranformation) 
    groupby
    join

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.filter("Year=2007")

# COMMAND ----------

df.filter("Year=2007").explain()

# COMMAND ----------

df.filter("Year=2007").display()

# COMMAND ----------

df.groupBy("Year").count()

# COMMAND ----------

df.groupBy("Year").count().explain()

# COMMAND ----------

df.groupBy("Year").count().display()

# COMMAND ----------


