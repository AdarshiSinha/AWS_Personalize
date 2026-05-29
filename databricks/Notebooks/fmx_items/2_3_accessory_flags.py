# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import requests

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
    map_table = "prd_mpv_map_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
    map_table = "prd_mpv_map_dev"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
    map_table = "prd_mpv_map_prd"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()
auth0_token = utils.get_auth0_token()

# COMMAND ----------

active_items_df = snowflake.execute_reader(
    f"select product_key from {db}.{schema}.fmx_all_products_raw where is_active"
).distinct()
product_keys = [p["PRODUCT_KEY"]for p in active_items_df.collect()]
accessory_flag_list = []
counter = 0
status_message = f"products out of {len(product_keys)} processed"
headers = {
    "Authorization": f"Bearer {auth0_token}",
    "Content-Type": "application/json"
}
for product_key in product_keys:
    url = f"https://accessory.products.cimpress.io/v1/accessory/{product_key}:current"
    response = requests.get(url, headers=headers)
    accessory_flag_list.append({
        "PRODUCT_KEY": product_key,
        "IS_ACCESSORY": 1 if response.json() else 0
    })
    counter += 1
    if counter % 100 == 0:
        print(f"{counter} {status_message}")
print(f"{counter} {status_message}")

# COMMAND ----------

accessory_flag_df = spark.createDataFrame(accessory_flag_list)

# COMMAND ----------

snowflake.table_from_df(
    df=accessory_flag_df,
    database=db,
    schema=schema,
    table="fmx_accessory_flags",
    mode="overwrite")
