# Databricks notebook source
# MAGIC %run "/Users/naval@thedatamaster.in/Eclerx/includes"

# COMMAND ----------

df=spark.read.json(f"{input_path}customers.json")

df.write.mode("overwrite").saveAsTable("bronze.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customers
