# Databricks notebook source
# MAGIC %run //Workspace/Users/shirin.lad@eclerx.com/eClerx/Eclerx/includes

# COMMAND ----------

dbutils.widgets.text("table", "sales")
dbutils.widgets.text("schema", "shirin_bronze")
dbutils.widgets.text("catalog", "dev")
table=dbutils.widgets.get("table")
schema=dbutils.widgets.get("schema")
catalog=dbutils.widgets.get("catalog")

# COMMAND ----------

df=spark.read.csv(f"{input_path}/{table}.csv",header=True,inferSchema=True)
df1=add_ingestion_date(df)
df1.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
