# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

input_path="dbfs:/FileStore/rawfiles/"
