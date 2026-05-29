# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

dbutils.widgets.text("filter", "")
filter = dbutils.widgets.get("filter")

print(f"""
filter: {filter}
""")

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

if filter == "accessory":
    expression = "INCLUDE ItemID WHERE Items.IS_ACCESSORY = 0"
elif filter == "logomaker":
    expression = "INCLUDE ItemID WHERE Items.IS_LOGOMAKER_ENABLED = 1"
elif filter == "category":
    expression = "INCLUDE ItemID WHERE Items.SITE_CATEGORY IN ($category)"
else:
    raise Exception(f"Invalid filter {filter}")

# COMMAND ----------

filter_arn = personalize.create_filter(
    name=f"{dataset_group_name}_{filter}_filter",
    datasetGroupArn=dataset_group_arn,
    filterExpression=expression
)["filterArn"]

dbutils.jobs.taskValues.set(key=f"{filter}_filter_arn", value=filter_arn)
