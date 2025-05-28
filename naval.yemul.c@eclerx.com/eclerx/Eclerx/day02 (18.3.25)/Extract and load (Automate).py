# Databricks notebook source
# DBTITLE 1,Extract/ Reading
df=spark.read.csv("dbfs:/FileStore/Adult_Census_Income.csv",header=True,inferSchema=True)

df.write.mode("overwrite").saveAsTable("test.adults_income")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.adults_income
