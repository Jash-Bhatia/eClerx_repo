# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.yemul.c@eclerx.com/Project/includes

# COMMAND ----------

customer_df = spark.table("bronze.customers")
product_df = spark.table("bronze.products")
sales_df = spark.table("bronze.sales")

# COMMAND ----------

get_shape(sales_df), get_shape(customer_df),get_shape(product_df)

# COMMAND ----------

sales_df_cleaned=sales_df.dropDuplicates().dropna()
customer_df_cleaned=customer_df.dropDuplicates().dropna()
product_df_cleaned=product_df.dropDuplicates().dropna()

# COMMAND ----------

get_shape(sales_df_cleaned), get_shape(customer_df_cleaned),get_shape(product_df_cleaned)

# COMMAND ----------

sales_df_cleaned.write.mode("overwrite").saveAsTable("silver.sales")
customer_df_cleaned.write.mode("overwrite").saveAsTable("silver.customers")
product_df_cleaned.write.mode("overwrite").saveAsTable("silver.products")
