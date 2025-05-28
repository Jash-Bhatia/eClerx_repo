# Databricks notebook source
1gb csv, 
(60%,80%, 97%)
 17mb Parquet

 60-80%


# COMMAND ----------

Delta lake: Open source storage framework. help you to build lakehouses. help you bring ACID to existing cloud storage.

Delta lake: extenstion of parquet

# COMMAND ----------

ways to create delta lake table : 
1. SQL 
    (CTAS/ CRTAS)
2. df=spark.read.csv("path")
    df.write.saveAsTable("tbalname")

3. Python    


# COMMAND ----------

# DBTITLE 1,Internal of Delta
internals of delta lake
Every transaction it will create log


1. parquet files  (Data)
 +
2. _delta_log
    i. crc (cycle redudnat check)
    ii. json
    iii. checkpoint parquet


# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales(id int, cust_id int, product_name string, amt int)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create a table. 
# MAGIC describe extended 
# MAGIC describe history
# MAGIC  view dbfs
# MAGIC
# MAGIC describe detial  
# MAGIC
# MAGIC
# MAGIC Delete few records: check the dbfs/ history/ detial
# MAGIC update few records: check the dbfs/ history/ detial
# MAGIC
# MAGIC time travel
# MAGIC
# MAGIC ctas

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales

# COMMAND ----------

# MAGIC %fs head
# MAGIC dbfs:/user/hive/warehouse/sales/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %fs head
# MAGIC dbfs:/user/hive/warehouse/sales/_delta_log/00000000000000000003.json

# COMMAND ----------

# MAGIC %fs head
# MAGIC dbfs:/user/hive/warehouse/sales/_delta_log/00000000000000000000.crc

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales values (1, 100, "iPhone", 1000)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales values (2, 101, "MacBook", 2000),(3, 102, "MacBook", 2000)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales @v1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales timestamp as of '2025-03-21T11:49:23.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales_v2 as 
# MAGIC select * from sales version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.sales_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC update sales
# MAGIC SET product_name='iMac'
# MAGIC where id=3

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sales where id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sales where id=2
