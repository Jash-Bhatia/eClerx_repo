# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML Driver Notebook
# MAGIC This is the parent notebook for AutoML.
# MAGIC This notebook is used to start all the trials when you run AutoML from the Experiments UI.
# MAGIC However, you typically do not need to modify or rerun this notebook.
# MAGIC Instead, you should go to the AutoML Experiment, which has links to the data exploration notebook and each trial notebook.

# COMMAND ----------

from typing import List

# conversion functions
def identity(param: str) -> str:
  return param

def to_int(param: str) -> int:
  return int(param)

def split_by_comma(param: str) -> List[str]:
  return param.split(",")

# COMMAND ----------

try:
  import torch
  cuda_available = torch.cuda.is_available()
  device_count = torch.cuda.device_count()
  print(f"is cuda available: {cuda_available}  device_count: {device_count}")
except:
  pass

# COMMAND ----------

from __future__ import annotations
from enum import Enum
from typing import Optional, Callable, Any, Set

class Param(Enum):
  widget_name: str
  automl_param_name: str
  from_string_fn: Optional[Callable[[str], Any]]

  def __init__(
    self,
    widget_name: str,
    fit_param_name: Optional[str],
    from_string: Callable[[str], Any]=identity
  ):
    self.widget_name = widget_name
    self.fit_param_name = fit_param_name
    self.from_string = from_string

  # top level param
  PROBLEM_TYPE = (
    "problemType",
    None,
  )

  # common params
  EXPERIMENT_ID = (
    "experimentId",
    None
  )
  TABLE_NAME = (
    "tableName",
    None
  )
  TARGET_COL = (
    "targetCol",
    "target_col"
  )
  TIME_COL = (
    "timeCol",
    "time_col"
  )
  PRIMARY_METRIC = (
    "primaryMetric",
    "metric"
  )
  EXPERIMENT_DIR = (
    "experimentDir",
    "home_dir"
  )
  DATA_DIR = (
    "dataDir",
    "data_dir"
  )
  TIMEOUT_MINUTES = (
    "timeoutMinutes",
    "timeout_minutes",
    to_int
  )
  MAX_TRIALS = (
    "maxTrials",
    "max_trials",
    to_int
  )
  EXCLUDE_FRAMEWORKS = (
    "excludeFrameworks",
    "exclude_frameworks",
    split_by_comma
  )

  SAMPLE_WEIGHT_COL = (
    "sampleWeightCol",
    "sample_weight_col"
  )

  # supervised learner params
  EXCLUDE_COLS = (
    "excludeCols",
    "exclude_columns",
    split_by_comma
  )

  SPLIT_COL = (
    "splitCol",
    "split_col"
  )

  # classification params
  POS_LABEL = (
    "posLabel",
    "pos_label"
  )

  # forecasting only params
  HORIZON = (
    "horizon",
    "horizon",
    to_int
  )
  FREQUENCY = (
    "frequency",
    "frequency"
  )
  IDENTITY_COLS = (
    "identityCols",
    "identity_col",
    split_by_comma
  )
  OUTPUT_DATABASE = (
    "outputDatabase",
    "output_database"
  )
  COUNTRY_CODE = (
    "countryCode",
    "country_code",
  )

# COMMAND ----------

# initialize widgets
for param in Param:
  dbutils.widgets.text(param.widget_name, "")

# COMMAND ----------

# fetch param values from widget
params = {}
for param in Param:
  value = dbutils.widgets.get(param.widget_name)
  if value != "":
    params[param] = param.from_string(value)
params

# COMMAND ----------

# MAGIC %md
# MAGIC Build the fit params

# COMMAND ----------

# override some param values
if Param.MAX_TRIALS not in params:
  params[Param.MAX_TRIALS] = 1e10

# COMMAND ----------

# start with values that can directly be input from the params
kwargs = {k.fit_param_name: v for k, v in params.items() if k.fit_param_name is not None}
kwargs

# COMMAND ----------

# read dataset from the given table name
table_name_segments = params[Param.TABLE_NAME].split('.')
escaped_table_name_segments = map(lambda s: f"`{s}`", table_name_segments)
escaped_table_name = '.'.join(escaped_table_name_segments)
dataset = spark.table(escaped_table_name)
kwargs["dataset"] = dataset
dataset.printSchema()

# COMMAND ----------

import mlflow
import json

experiment = mlflow.get_experiment(params[Param.EXPERIMENT_ID])
kwargs["experiment"] = experiment

if "_databricks_automl.imputers" in experiment.tags:
    imputers = json.loads(experiment.tags["_databricks_automl.imputers"])
    if imputers:
        kwargs["imputers"] = imputers

if "_databricks_automl.feature_store_lookups" in experiment.tags:
    feature_store_lookups = json.loads(experiment.tags["_databricks_automl.feature_store_lookups"])
    if feature_store_lookups:
        kwargs["feature_store_lookups"] = feature_store_lookups

kwargs

# COMMAND ----------

from databricks.automl import ContextType

problem_type = params[Param.PROBLEM_TYPE]

if problem_type == "classification":
  from databricks.automl.classifier import Classifier
  classifier = Classifier(context_type=ContextType.DATABRICKS)
  classifier.fit(**kwargs)
elif problem_type == "regression":
  from databricks.automl.regressor import Regressor
  regressor = Regressor(context_type=ContextType.DATABRICKS)
  regressor.fit(**kwargs)
elif problem_type == "forecasting":
  from databricks.automl.forecast import Forecast
  forecastor = Forecast(context_type=ContextType.DATABRICKS)
  forecastor.fit(**kwargs)
else:
  raise ValueError(f"Unknown problem_type: {problem_type}")

# COMMAND ----------


