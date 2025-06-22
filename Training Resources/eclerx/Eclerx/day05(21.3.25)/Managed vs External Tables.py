# Databricks notebook source
Table in schema +  metadata(parquet(data)+ delta log(json ,crc etc)
                            

DROP

Managed table (table gets deleted from schema & metadata gets deleted )

ext /unManaged table (table gets deleted from schema & metaata DOES NOT delete )

# COMMAND ----------

CTAS

df

python api 

# COMMAND ----------

# DBTITLE 1,managed
# MAGIC %sql
# MAGIC create table emp (id int, name string) 

# COMMAND ----------

# DBTITLE 1,Managed
df=spark.read.json("path")
df.write.saveAsTable("tbalname")

# COMMAND ----------

# DBTITLE 1,Managed
# MAGIC %sql
# MAGIC create table emp_new as select * from emp

# COMMAND ----------

# DBTITLE 1,unmanaged
# MAGIC %sql
# MAGIC create table emp_new location ' adls' as select * from emp

# COMMAND ----------

# DBTITLE 1,unmanged
# MAGIC %sql
# MAGIC create table emp (id int, name string) location ''

# COMMAND ----------

# DBTITLE 1,Unmanaged table
df=spark.read.json("path")
df.write.option("path","adls:").saveAsTable("tbalname")

# COMMAND ----------

df=spark.read.json("path")
df.write.option("path","adls:").format('parquet').saveAsTable("tbalname")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp (id int, name string) 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_vendor (id int, name string) location '/FileStore/vendor/emp'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_vendor values (1,'Aditya')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp_vendor

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP Table emp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP Table emp_vendor

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/FileStore/vendor/emp`

# COMMAND ----------

# MAGIC %sql
# MAGIC undrop table emp

# COMMAND ----------


