# Databricks notebook source
# MAGIC %md
# MAGIC ![](dbfs:/FileStore/object_model_0ed879da6c005615e8a00db9bb10823c.png)

# COMMAND ----------

Schema/Databases
Tables, Views, Volumes, Functions, Model 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists test.demo (id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test.demo values (1,'Naval')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended test.demo
