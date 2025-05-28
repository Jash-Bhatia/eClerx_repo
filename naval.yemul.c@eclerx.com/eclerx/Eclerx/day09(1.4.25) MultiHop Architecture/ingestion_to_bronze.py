# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.yemul.c@eclerx.com/eclerx/Eclerx/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)
df1=add_ingestion_date(df)
df1.write.mode("overwrite").saveAsTable("dev.naval_bronze.sales")
