# Databricks notebook source
Views: A virtual table 
    1. Standard View (Persisted) (SQL)
    2. Temp View (sql, pyspark) (valid only in that spark session)
    3. Global Temp View (sql, pyspark)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.sales_silver as 
# MAGIC select distinct * from hive_metastore.bronze.sales where order_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view gold.customer_total_amount as 
# MAGIC (select customer_id, round(sum(total_amount)) as sum_total_amount from hive_metastore.silver.sales_silver group by customer_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.customer_total_amount

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended gold.customer_total_amount

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view sales_silver_product as 
# MAGIC select distinct product_id from hive_metastore.silver.sales_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view sales_silver_product_global as 
# MAGIC select distinct product_id from hive_metastore.silver.sales_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in gold

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.sales_silver_product_global

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_silver_product

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/rawfiles/customers.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("customer_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from df
# MAGIC create or replace table bronze.customer as 
# MAGIC select *, current_date as ingestion_date from customer_temp

# COMMAND ----------

Q. A data engineer wants to create a data entity from a couple of tables. The data entity must be used by other data engineers in other sessions. It also must be saved to a physical location.
Which of the following data entities should the data engineer create?

A. Database
B. Function
C. View
D. Temporary view
E. Table


# COMMAND ----------

Q.A data engineer is attempting to drop a Spark SQL table my_table. The data engineer wants to delete all table metadata and data.
They run the following command:

DROP TABLE IF EXISTS my_table -
While the object no longer appears when they run SHOW TABLES, the data files still exist.
Which of the following describes why the data files still exist and the metadata files were deleted?
A. The table’s data was larger than 10 GB
B. The table’s data was smaller than 10 GB
C. The table was external
D. The table did not have a location
E. The table was managed


# COMMAND ----------

After running DESCRIBE EXTENDED accounts.customers;, the following was returned:

Now, a data analyst runs the following command:
DROP accounts.customers;
Which of the following describes the result of running this command?
A. Running SELECT * FROM delta. `dbfs:/stakeholders/customers` results in an error.
B. Running SELECT * FROM accounts.customers will return all rows in the table.
C. All files with the .customers extension are deleted.
D. The accounts.customers table is removed from the metastore, and the underlying data files are deleted.
E. The accounts.customers table is removed from the metastore, but the underlying data files are untouched.

