# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_vf_1;
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import pandas as pd
input_path = "/Volumes/demo_vf_1/default/raw" 

# COMMAND ----------

def get_shape(dataframe):
    return dataframe.count(), len(dataframe.columns)
