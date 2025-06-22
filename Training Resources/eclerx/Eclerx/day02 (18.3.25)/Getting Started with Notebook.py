# Databricks notebook source
# DBTITLE 1,Azure pricing link
https://azure.microsoft.com/en-us/pricing/details/databricks/

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook
# MAGIC

# COMMAND ----------

# DBTITLE 1,python
#print("Python")

# COMMAND ----------

# DBTITLE 1,sql
# MAGIC %sql
# MAGIC --select "Run SQL"

# COMMAND ----------

# DBTITLE 1,scala
# MAGIC %scala
# MAGIC println("RUN Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC # Db utility

# COMMAND ----------

https://docs.databricks.com/aws/en/dev-tools/databricks-utils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/auto_checkpoint",True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Databases/Schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists test

# COMMAND ----------

# MAGIC %sql
# MAGIC create database naval

# COMMAND ----------


