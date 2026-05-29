# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

account_id = "arn:aws:personalize:eu-west-1:905666450942"
dataset_types = ["items", "users"]

dataset_group_list = [dg["name"] for dg in personalize.list_dataset_groups()["datasetGroups"]]
if environment == "prd":
    dataset_groups_to_update = [dg for dg in dataset_group_list if "test" not in dg]
else:
    dataset_groups_to_update = [dg for dg in dataset_group_list if "test" in dg]

# COMMAND ----------

# This will list all available dataset groups in our account.
# For each dataset group it will generate dataset ARN and pass it to the next task.
# Next task is 3_import_data, it will fully refresh items and users dataset.
# It will be executed in a loop, all tasks will run in parallel.
# Possible improvements - restrict update to specific version

all_datasets_data = []
for dataset_group in dataset_groups_to_update:
    for dataset_type in dataset_types:
        dataset_data = {
            "dataset_group_name": dataset_group,
            "dataset_type": dataset_type,
            "dataset_arn": f"{account_id}:dataset/{dataset_group}/{dataset_type.upper()}"
        }
        all_datasets_data.append(dataset_data)

print("Dataset groups to update:")
print("\n".join(dataset_groups_to_update))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="dataset_params", value=all_datasets_data)
