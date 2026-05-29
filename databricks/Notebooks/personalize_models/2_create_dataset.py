# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

import json
from time import sleep
import importlib

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("version", "")
version = dbutils.widgets.get("version")
if not version:
    raise Exception("Invalid version")

dbutils.widgets.text("dataset_type", "")
dataset_type = dbutils.widgets.get("dataset_type")
if dataset_type not in ("items", "users", "interactions"):
    raise Exception("Invalid dataset type")

dbutils.widgets.text("dataset_group_name", "")
dataset_group_name = dbutils.widgets.get("dataset_group_name")
if not dataset_group_name:
    dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

dbutils.widgets.text("dataset_group_arn", "")
dataset_group_arn = dbutils.widgets.get("dataset_group_arn")
if not dataset_group_arn:
    dataset_group_arn = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_arn")

print(f"""
environment: {environment}
dataset_group_name: {dataset_group_name}
dataset_group_arn: {dataset_group_arn}
dataset_type: {dataset_type}
version: {version}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

# MAGIC %run ./load_versions

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

schema = definitions[version][dataset_type]["schema"]
personalize_schema = json.dumps(schema)

# COMMAND ----------

dataset_name = f"{dataset_group_name}_{dataset_type}"
schema_name = f"{dataset_group_name}_{dataset_type}_schema"

# COMMAND ----------

schema_arn = personalize.create_schema(
    name=schema_name,
    schema=personalize_schema
)["schemaArn"]

# COMMAND ----------

dataset_arn = personalize.create_dataset(
    name=dataset_name,
    schemaArn=schema_arn,
    datasetGroupArn=dataset_group_arn,
    datasetType=dataset_type
)["datasetArn"]

# COMMAND ----------

status = None
while status != "ACTIVE":
    status = personalize.describe_dataset(datasetArn=dataset_arn)["dataset"]["status"]
    if status == "CREATE FAILED":
        raise Exception(f"Error while creating {dataset_arn}")
    sleep(30)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="dataset_arn", value=dataset_arn)
