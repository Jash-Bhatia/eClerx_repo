# Databricks notebook source
https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

#df=spark.read.csv("dbfs:/FileStore/Adult_Census_Income.csv")

# COMMAND ----------

#df=spark.read.csv("dbfs:/FileStore/Adult_Census_Income.csv",header=True)

# COMMAND ----------

# DBTITLE 1,Extract/ Reading
df=spark.read.csv("dbfs:/FileStore/Adult_Census_Income.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

#df.show()

# COMMAND ----------

df.write.saveAsTable("test.adults_income")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended test.adults_income

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.adults_income where workclass='Private'
