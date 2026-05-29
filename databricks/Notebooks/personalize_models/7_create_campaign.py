# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from time import sleep

dbutils.widgets.text("solution", "")
solution = dbutils.widgets.get("solution")

if solution not in ("sims", "popular", "up"):
    raise Exception(f"Invalid solution {solution}")

print(f"""
solution: {solution}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

solution_arn = dbutils.jobs.taskValues.get(taskKey=f"create_solution_{solution}", key="solution_arn")
dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")
campaign_name = f"{dataset_group_name}_{solution}_sol_cpn"
latest_solution_version = f"{solution_arn}/$LATEST"
tps = 1
if solution == "up":
    campaign_config = {
        "syncWithLatestSolutionVersion": True,
        "itemExplorationConfig": {
            "explorationWeight": "0.3",
            "explorationItemAgeCutOff": "30"
        }
    }
else:
    campaign_config = {"syncWithLatestSolutionVersion": True}

# COMMAND ----------

def check_campaign_status(campaign_arn, status=None):
    while status != "ACTIVE":
        status = personalize.describe_campaign(campaignArn=campaign_arn)["campaign"]["status"]
        if status == "CREATE FAILED":
            raise Exception(f"Error while creating {campaign_arn}")
        sleep(30)

campaigns = personalize.list_campaigns(solutionArn=solution_arn)["campaigns"]

if campaigns:
    campaign_arn = campaigns[0]["campaignArn"]
    status = campaigns[0]["status"]
    check_campaign_status(campaign_arn, status=status)
else:
    campaign_arn = personalize.create_campaign(
        name=campaign_name,
        solutionVersionArn=latest_solution_version,
        minProvisionedTPS=tps,
        campaignConfig=campaign_config,
    )["campaignArn"]
    check_campaign_status(campaign_arn)

# COMMAND ----------

dbutils.jobs.taskValues.set(key=f"{solution}_sol_cpn_arn", value=campaign_arn)
