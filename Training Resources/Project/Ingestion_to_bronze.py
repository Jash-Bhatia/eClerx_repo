# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval.yemul.c@eclerx.com/Project/includes"

# COMMAND ----------

import pandas as pd
input_path = "/Volumes/demo_vf_1/default/raw"

# COMMAND ----------

# DBTITLE 1,sales_df
sales_pd = pd.read_excel(f'{input_path}/sales.xlsx',engine='openpyxl')
sales_df = spark.createDataFrame(sales_pd)

# COMMAND ----------

# DBTITLE 1,product_df
product_pd = pd.read_excel(f'{input_path}/product.xlsx',engine='openpyxl')
product_df = spark.createDataFrame(product_pd)

# COMMAND ----------

# DBTITLE 1,customer_df
customer_pd = pd.read_excel(f'{input_path}/customer.xlsx',engine='openpyxl')
customer_df=spark.createDataFrame(customer_pd)

# COMMAND ----------

sales_df.write.mode("overwrite").saveAsTable("bronze.sales")
product_df.write.mode("overwrite").saveAsTable("bronze.products")
customer_df.write.mode("overwrite").saveAsTable("bronze.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct * from demo_vf_1.bronze.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from demo_vf_1.bronze.customer 

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from demo_vf_1.bronze.product
