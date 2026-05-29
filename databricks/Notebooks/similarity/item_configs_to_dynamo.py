# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import json
from pyspark.sql.functions import collect_list
import requests

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
    item_configs_table = "item_configs_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
    item_configs_table = "item_configs_dev"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
    item_configs_table = "item_configs_prd"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

# MAGIC %run ./query

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)

dynamo = session.resource('dynamodb', region_name="eu-west-1")
dynamo_item_configs_table = dynamo.Table(item_configs_table)

# COMMAND ----------

constraints_query = Query({"recs_region": "dummy", "limit_count": "dummy"}).constraints_query
constraints_df = snowflake.execute_reader(constraints_query).cache()

# COMMAND ----------

universe_df = snowflake.execute_reader(f"""
select distinct
  source_product_key,
  source_product_version,
  market as source_market,
  source_color,
  source_user_selections_config_only
from
  {db}.{schema}.product_similarity_universe
"""
).selectExpr(
  "SOURCE_PRODUCT_KEY",
  "SOURCE_PRODUCT_VERSION",
  "SOURCE_MARKET",
  "SOURCE_COLOR",
  "from_json(SOURCE_USER_SELECTIONS_CONFIG_ONLY, 'map<string, string>') as ITEM_CONFIG"
).cache()

# COMMAND ----------

narrowed_constraints_df = constraints_df.join(universe_df, ["SOURCE_PRODUCT_KEY", "SOURCE_PRODUCT_VERSION", "SOURCE_MARKET"], "left_semi")
print(f"Potentially constrained items count: {narrowed_constraints_df.count()}")

final_constraints_df = narrowed_constraints_df.cache()

snowflake.table_from_df(
    df=final_constraints_df,
    database=db,
    schema=schema,
    table=f"constrained_substitution_items",
    mode="overwrite")

# COMMAND ----------

universe_wo_constraints_df = universe_df \
    .join(final_constraints_df.where("SOURCE_COLOR = 'ALL_COLORS'"), ["SOURCE_PRODUCT_KEY", "SOURCE_PRODUCT_VERSION", "SOURCE_MARKET"], "left_anti") \
    .join(final_constraints_df.where("SOURCE_COLOR <> 'ALL_COLORS'"), ["SOURCE_PRODUCT_KEY", "SOURCE_PRODUCT_VERSION", "SOURCE_MARKET", "SOURCE_COLOR"], "left_anti")

# COMMAND ----------

item_configs_df = universe_wo_constraints_df \
    .groupBy("SOURCE_PRODUCT_KEY", "SOURCE_PRODUCT_VERSION", "SOURCE_MARKET") \
    .agg(collect_list("ITEM_CONFIG").alias("ITEM_CONFIGS"))

new_dynamo_items = [
    {
        'locale_item_version': f'{row["SOURCE_MARKET"]}|{row["SOURCE_PRODUCT_KEY"]}|{row["SOURCE_PRODUCT_VERSION"]}'.upper(),
        'item_configs': json.dumps(row["ITEM_CONFIGS"])
    }
    for row in item_configs_df.collect()
]

# COMMAND ----------

scan_response = dynamo_item_configs_table.scan()
current_dynamo_items = scan_response['Items']
while scan_response.get("LastEvaluatedKey"):
    scan_response = dynamo_item_configs_table.scan(ExclusiveStartKey=scan_response['LastEvaluatedKey'])
    current_dynamo_items.extend(scan_response['Items'])

current_dynamo_keys = set(i['locale_item_version'] for i in current_dynamo_items)
new_dynamo_keys = set(i['locale_item_version'] for i in new_dynamo_items)
keys_to_delete = current_dynamo_keys - new_dynamo_keys

print(f"{len(current_dynamo_keys)} item configs currently stored")
print(f"{len(keys_to_delete)} item configs will be deleted")
print(f"{len(new_dynamo_items)} item configs will be inserted")

# COMMAND ----------

with dynamo_item_configs_table.batch_writer() as batch:
    for key_to_delete in keys_to_delete:
        batch.delete_item(Key={'locale_item_version': key_to_delete})

# COMMAND ----------

with dynamo_item_configs_table.batch_writer() as batch:
    for item in new_dynamo_items:
        batch.put_item(Item={
            'locale_item_version': item['locale_item_version'],
            'item_configs': item['item_configs']
        })
