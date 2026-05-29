# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from datetime import datetime, timezone
from time import sleep

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("region", "")
region = dbutils.widgets.get("region")
if region not in ["us", "eu", "ca", "in", "anzs", "dach", "frcen", "uk"]:
    raise ValueError("correct region is not set")

dbutils.widgets.text("version", "")
version = dbutils.widgets.get("version")
if not version:
    raise ValueError("version is not set")

dbutils.widgets.text("poet_model_version", "")
poet_model_version = dbutils.widgets.get("poet_model_version")
if not poet_model_version:
    raise ValueError("poet_model_version is not set")

print(f"""
environment: {environment}
region: {region}
version: {version}
poet_model_version: {poet_model_version}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)
ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
dataset_group_name = f"{region}_{ts}_{version}".lower()
dataset_group_arn = personalize.create_dataset_group(name=dataset_group_name)["datasetGroupArn"]

# COMMAND ----------

status = None
while status != "ACTIVE":
    status = personalize.describe_dataset_group(datasetGroupArn=dataset_group_arn)["datasetGroup"]["status"]
    if status == "CREATE FAILED":
        raise Exception(f"Error while creating {dataset_group_arn}")
    sleep(30)

# COMMAND ----------

personalize.tag_resource(
    resourceArn=dataset_group_arn,
    tags=[{'tagKey': 'poet_model_version', 'tagValue': poet_model_version}]
)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="dataset_group_arn", value=dataset_group_arn)
dbutils.jobs.taskValues.set(key="dataset_group_name", value=dataset_group_name)
