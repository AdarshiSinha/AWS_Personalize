# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from time import sleep

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

dataset_group_arn = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_arn")
dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

# COMMAND ----------

response = personalize.create_event_tracker(
    name=f"{dataset_group_name}_et",
    datasetGroupArn=dataset_group_arn)

event_tracker_arn = response["eventTrackerArn"]
tracking_id = response["trackingId"]

# COMMAND ----------

status = None
while status != "ACTIVE":
    status = personalize.describe_event_tracker(eventTrackerArn=event_tracker_arn)["eventTracker"]["status"]
    if status == "CREATE FAILED":
        raise Exception(f"Error while creating {event_tracker_arn}")
    sleep(30)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="event_tracker_arn", value=event_tracker_arn)
dbutils.jobs.taskValues.set(key="tracking_id", value=tracking_id)
