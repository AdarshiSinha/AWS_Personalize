# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from time import sleep
from pprint import pprint

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("dataset_group", "")
dataset_group = dbutils.widgets.get("dataset_group")
if not dataset_group:
    raise ValueError("dataset_group is not set")

region, dataset_version, model_version = dataset_group.split("_")

print(f"""
environment: {environment}
dataset_group: {dataset_group}
region: {region}
model_version: {model_version}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials(duration=4*3600)
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

def list_dataset_groups():
    return [dg["name"] for dg in personalize.list_dataset_groups()["datasetGroups"]]

def print_dataset_groups():
    pprint([f'{d["name"]} -> {d["status"]}' for d in personalize.list_dataset_groups()["datasetGroups"]])

if dataset_group not in list_dataset_groups():
    print_dataset_groups()
    raise Exception(f"Dataset group {dataset_group} does not exist")

# COMMAND ----------

account_id = "arn:aws:personalize:eu-west-1:905666450942"
dataset_group_arn = f"{account_id}:dataset-group/{dataset_group}"
solution_arns = [
    f"{account_id}:solution/{dataset_group}_up_sol",
    f"{account_id}:solution/{dataset_group}_sims_sol",
    f"{account_id}:solution/{dataset_group}_popular_sol",
]
dataset_arns = [
    f"{account_id}:dataset/{dataset_group}/ITEMS",
    f"{account_id}:dataset/{dataset_group}/USERS",
    f"{account_id}:dataset/{dataset_group}/INTERACTIONS",
]
poet_model_version = personalize.list_tags_for_resource(resourceArn=dataset_group_arn)["tags"][0]["tagValue"]

# COMMAND ----------

def filter_exists():
    return filter_arn in [f["filterArn"] for f in personalize.list_filters(datasetGroupArn=dataset_group_arn)["Filters"]]

def print_filters():
    pprint([f'{f["name"]} -> {f["status"]}' for f in personalize.list_filters(datasetGroupArn=dataset_group_arn)["Filters"]])

for filter_type in ["logomaker", "accessory", "category"]:
    print(f"Deleting {filter_type} filter...")
    try:
        filter_arn = f"{account_id}:filter/{dataset_group}_{filter_type}_filter"
        personalize.delete_filter(filterArn=filter_arn)
        while filter_exists():
            print_filters()
            sleep(30)
    except:
        print(f"Not possible to delete {filter_type} filter, current filter list:")
        print_filters()

# COMMAND ----------

def campaign_exists(solution_arn):
    return campaign_arn in [c["campaignArn"] for c in personalize.list_campaigns(solutionArn=solution_arn)["campaigns"]]

def print_campaigns(solution_arn):
    pprint([f'{c["name"]} -> {c["status"]}' for c in personalize.list_campaigns(solutionArn=solution_arn)["campaigns"]])

for campaign in ["up", "sims", "popular"]:
    print(f"Deleting {campaign} campaign...")
    solution_arn = f"{account_id}:solution/{dataset_group}_{campaign}_sol"
    try:
        campaign_arn = f"{account_id}:campaign/{dataset_group}_{campaign}_sol_cpn"
        personalize.delete_campaign(campaignArn=campaign_arn)
        while campaign_exists(solution_arn):
            print_campaigns(solution_arn)
            sleep(30)
    except:
        print(f"Not possible to delete {campaign} campaign, current campaign list:")
        print_campaigns(solution_arn)

# COMMAND ----------

def solution_exists():
    return solution_arn in [s["solutionArn"] for s in personalize.list_solutions(datasetGroupArn=dataset_group_arn)["solutions"]]

def print_solutions():
    pprint([f'{s["name"]} -> {s["status"]}' for s in personalize.list_solutions(datasetGroupArn=dataset_group_arn)["solutions"]])

for solution_arn in solution_arns:
    print(f"Deleting {solution_arn.split('/')[-1]} solution...")
    try:
        personalize.delete_solution(solutionArn=solution_arn)
        while solution_exists():
            print_solutions()
            sleep(30)
    except:
        print(f"Not possible to delete {solution_arn.split('/')[-1]} solution, current solution list:")
        print_solutions()

# COMMAND ----------

temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
dynamo = session.resource("dynamodb", region_name="eu-west-1")

if environment == "prd":
    tracker_tables = ["locale_version_tracker_map_dev", "locale_version_tracker_map_prd"]
else:
    tracker_tables = ["locale_version_tracker_map_dev"]

for dynamo_table in tracker_tables:
    print(f"Deleting tracking ids from {dynamo_table}...")
    tracking_map_dynamo_table = dynamo.Table(dynamo_table)
    with tracking_map_dynamo_table.batch_writer() as batch:
        for locale in utils.region_locale[region]:
            key = {"locale": locale, "version": dataset_group}
            print(f"Deleting tracking map {key}")
            batch.delete_item(Key=key)

# COMMAND ----------

def event_tracker_list():
    return personalize.list_event_trackers(datasetGroupArn=dataset_group_arn)["eventTrackers"]

def print_event_tracker():
    pprint([f'{et["name"]} -> {et["status"]}' for et in personalize.list_event_trackers(datasetGroupArn=dataset_group_arn)["eventTrackers"]])
print(f"Deleting event tracker...")
for et in event_tracker_list():
    print_event_tracker()
    personalize.delete_event_tracker(eventTrackerArn=et["eventTrackerArn"])
while event_tracker_list():
    print_event_tracker()
    sleep(30)

# COMMAND ----------

def dataset_exists():
    return dataset_arn in [ds["datasetArn"] for ds in personalize.list_datasets(datasetGroupArn=dataset_group_arn)["datasets"]]

def print_datasets():
    pprint([f'{ds["name"]} -> {ds["status"]}' for ds in personalize.list_datasets(datasetGroupArn=dataset_group_arn)["datasets"]])

for dataset_arn in dataset_arns:
    print(f"Deleting {dataset_arn.split('/')[-1]} dataset...")
    try:
        personalize.delete_dataset(datasetArn=dataset_arn)
        while dataset_exists():
            print_datasets()
            sleep(10)
    except:
        print(f"Not possible to delete {dataset_arn.split('/')[-1]} dataset, current dataset list:")
        print_datasets()

# COMMAND ----------

print(f"Deleting {dataset_group} dataset group...")
personalize.delete_dataset_group(datasetGroupArn=dataset_group_arn)

# COMMAND ----------

schemas = []
response = personalize.list_schemas()
schemas.extend(response["schemas"])
while "nextToken" in response:
    response = personalize.list_schemas(nextToken=response["nextToken"])
    schemas.extend(response["schemas"])
schema_arns = [s["schemaArn"] for s in schemas]

for schema_arn in schema_arns:
    if dataset_group in schema_arn:
        print(f"Deleting schema {schema_arn}...")
        personalize.delete_schema(schemaArn=schema_arn)

# COMMAND ----------

model_ids = [f"{region}_{model_version}_{postfix}" for postfix in [
    "sims",
    "user_personalization",
    "user_personalization_category_filter",
    "user_personalization_logomaker_filter",
    "most_popular"
]]

if environment == "prd":
    campaign_tables = ["version_campaign_filter_map_dev", "version_campaign_filter_map_prd"]
else:
    campaign_tables = ["version_campaign_filter_map_dev"]

for dynamo_table in campaign_tables:
    print(f"Deleting models from {dynamo_table}...")
    campaign_map_dynamo_table = dynamo.Table(dynamo_table)
    with campaign_map_dynamo_table.batch_writer() as batch:
        for model_id in model_ids:
            key = {"modelId": model_id, "version": poet_model_version}
            print(f"Deleting model {key}")
            campaign_arn_row = campaign_map_dynamo_table.get_item(Key=key).get("Item", {}).get("campaignArn", "")
            if dataset_group in campaign_arn_row:
                batch.delete_item(Key=key)

# COMMAND ----------

cw_client = session.client("cloudwatch", region_name="eu-west-1")

all_dashboards = cw_client.list_dashboards()['DashboardEntries']
dashboard_to_delete = [d['DashboardName'] for d in all_dashboards if dataset_group in d['DashboardName']]
if dashboard_to_delete:
    print(f"Deleting dashboards:")
    pprint(dashboard_to_delete)
    cw_client.delete_dashboards(DashboardNames=dashboard_to_delete)

all_alarms = cw_client.describe_alarms()['MetricAlarms']
alarms_to_delete = [a['AlarmName'] for a in all_alarms if dataset_group in a['AlarmName']]
if alarms_to_delete:
    print(f"Deleting alarms:")
    pprint(alarms_to_delete)
    cw_client.delete_alarms(AlarmNames=alarms_to_delete)
