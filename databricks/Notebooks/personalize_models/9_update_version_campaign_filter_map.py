# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from pprint import pprint

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("region", "")
region = dbutils.widgets.get("region")
if not region:
    raise ValueError("region is not set")

dbutils.widgets.text("version", "")
version = dbutils.widgets.get("version")
if not version:
    raise ValueError("version is not set")

dbutils.widgets.text("poet_model_version", "")
poet_model_version = dbutils.widgets.get("poet_model_version")
if poet_model_version:
    dynamo_map_model_version = f"{region}_{poet_model_version}"
else:
    raise ValueError("poet_model_version is not set")

dbutils.widgets.text("popular_sol_cpn_arn", "")
popular_sol_cpn_arn = dbutils.widgets.get("popular_sol_cpn_arn")
if not popular_sol_cpn_arn:
    popular_sol_cpn_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_popular", key="popular_sol_cpn_arn")

dbutils.widgets.text("up_sol_cpn_arn", "")
up_sol_cpn_arn = dbutils.widgets.get("up_sol_cpn_arn")
if not up_sol_cpn_arn:
    up_sol_cpn_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_up", key="up_sol_cpn_arn")

dbutils.widgets.text("sims_sol_cpn_arn", "")
sims_sol_cpn_arn = dbutils.widgets.get("sims_sol_cpn_arn")
if not sims_sol_cpn_arn:
    sims_sol_cpn_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_sims", key="sims_sol_cpn_arn")

dbutils.widgets.text("logomaker_filter_arn", "")
logomaker_filter_arn = dbutils.widgets.get("logomaker_filter_arn")
if not logomaker_filter_arn:
    logomaker_filter_arn = dbutils.jobs.taskValues.get(taskKey="create_filter_logomaker", key="logomaker_filter_arn")

dbutils.widgets.text("category_filter_arn", "")
category_filter_arn = dbutils.widgets.get("category_filter_arn")
if not category_filter_arn:
    category_filter_arn = dbutils.jobs.taskValues.get(taskKey="create_filter_category", key="category_filter_arn")

dbutils.widgets.text("accessory_filter_arn", "")
accessory_filter_arn = dbutils.widgets.get("accessory_filter_arn")
if not accessory_filter_arn:
    accessory_filter_arn = dbutils.jobs.taskValues.get(taskKey="create_filter_accessory", key="accessory_filter_arn")

print(f"""
environment: {environment}
poet_model_version: {poet_model_version}
dynamo_map_model_version: {dynamo_map_model_version}
region: {region}
version: {version}
popular_sol_cpn_arn: {popular_sol_cpn_arn}
sims_sol_cpn_arn: {sims_sol_cpn_arn}
up_sol_cpn_arn: {up_sol_cpn_arn}
logomaker_filter_arn: {logomaker_filter_arn}
category_filter_arn: {category_filter_arn}
accessory_filter_arn: {accessory_filter_arn}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

# COMMAND ----------

# The idea is to replace models.json file that is defined in scoring lambda repo with dyanmo db table.
# The file contains data about each campaign/filter configuration, for example:
# {
#     "modelId": "us_v4_sims",
#     "version": "may_2024",
#     "campaignArn": "arn:aws:personalize:eu-west-1:905666450942:campaign/us_20240523153317_v4_sims_sol_cpn",
#     "filterArn": "arn:aws:personalize:eu-west-1:905666450942:filter/us_20240523153317_v4_accessory_filter"
# }
# It indicates that v4 sims model for USA uses us_20240523153317_v4_sims_sol_cpn campaign and
# us_20240523153317_v4_accessory_filter filter.
# Storing this info in dynamo db will eliminate manual edition of this file after each model creation.

version_campaign_filter_map = [
    {
        "modelId": f"{region}_{version}_sims",
        "version": poet_model_version,
        "campaignArn": sims_sol_cpn_arn,
        "filterArn": accessory_filter_arn
    },
    {
        "modelId": f"{region}_{version}_user_personalization",
        "version": poet_model_version,
        "campaignArn": up_sol_cpn_arn,
        "filterArn": accessory_filter_arn
    },
    {
        "modelId": f"{region}_{version}_user_personalization_category_filter",
        "version": poet_model_version,
        "campaignArn": up_sol_cpn_arn,
        "filterArn": category_filter_arn
    },
    {
        "modelId": f"{region}_{version}_user_personalization_logomaker_filter",
        "version": poet_model_version,
        "campaignArn": up_sol_cpn_arn,
        "filterArn": logomaker_filter_arn
    },
    {
        "modelId": f"{region}_{version}_most_popular",
        "version": poet_model_version,
        "campaignArn": popular_sol_cpn_arn,
        "filterArn": accessory_filter_arn
    }
]

pprint(version_campaign_filter_map)

# COMMAND ----------

temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
dynamo = session.resource("dynamodb", region_name="eu-west-1")
dynamo_table = dynamo.Table(utils.version_campaign_filter_map_table)

with dynamo_table.batch_writer() as batch:
    for item in version_campaign_filter_map:
        batch.put_item(Item=item)
