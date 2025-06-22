# Databricks notebook source
Lazy evaluation 
Transformation Action

# COMMAND ----------

df=spark.table("hive_metastore.bronze.sales")

# COMMAND ----------

Transformation 
select
filter
sort
distinct
join
group by

# COMMAND ----------

Actions
show
display
count
write

# COMMAND ----------

DataFrame functions 
.select
.alias
.withColumnRenamed
.withColumnsRenamed
.withColumn

functions
col
current_date
current_timestamp


# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id as orderid

# COMMAND ----------

df.select("order_id","customer_id").display()

# COMMAND ----------

df.select("order_id".alias("orderid"),"customer_id").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("order_id").alias("orderid"),"customer_id").display()

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("order_id","orderId").display()

# COMMAND ----------

df.withColumnRenamed("order_id","orderId").withColumnRenamed("customer_id","custId").display()

# COMMAND ----------

df.withColumnsRenamed({"order_id":"orderId","customer_id":"custId"})

# COMMAND ----------

df.withColumn("order_id",current_timestamp()).display()

# COMMAND ----------

df.display()

# COMMAND ----------


