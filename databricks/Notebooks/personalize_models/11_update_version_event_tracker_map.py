# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("region", "")
region = dbutils.widgets.get("region")
if not region:
    raise ValueError("region is not set")

dbutils.widgets.text("dataset_group_name", "")
dataset_group_name = dbutils.widgets.get("dataset_group_name")
if not dataset_group_name:
    dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

dbutils.widgets.text("tracking_id", "")
tracking_id = dbutils.widgets.get("tracking_id")
if not tracking_id:
    tracking_id = dbutils.jobs.taskValues.get(taskKey="create_event_tracker", key="tracking_id")

print(f"""
environment: {environment}
region: {region}
dataset_group_name: {dataset_group_name}
tracking_id: {tracking_id}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
dynamo = session.resource("dynamodb", region_name="eu-west-1")
dynamo_table = dynamo.Table(utils.locale_version_tracker_map_table)

# COMMAND ----------

for locale in utils.region_locale[region]:
    item = {
        'locale': locale,
        'version': dataset_group_name,
        'tracking_id': tracking_id
    }
    print(item)
    dynamo_table.put_item(Item=item)
