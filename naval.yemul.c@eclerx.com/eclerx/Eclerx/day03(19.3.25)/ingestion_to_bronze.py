# Databricks notebook source
# MAGIC %run /Users/naval@thedatamaster.in/Eclerx/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("bronze.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.bronze.sales
