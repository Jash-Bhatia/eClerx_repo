# Databricks notebook source
# MAGIC %sql
# MAGIC select * from sales_silver_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.gold.customer_total_amount

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.sales_silver_product_global
