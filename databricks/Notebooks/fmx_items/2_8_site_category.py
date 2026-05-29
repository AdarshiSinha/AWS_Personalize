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

# COMMAND ----------

category_url = "https://product-hierarchy.prod.merch.vpsvc.com/v3/list/vistaprint/{culture}?requestor=precs"
item_category_list = []
category_map = {}

for locale, culture in utils.region_code.items():
    url = category_url.format(culture=culture)
    response = requests.get(url).json()
    
    for item in response:
        if item.get('type') == 'category':
            category_map[item.get('id')] = item.get('parentId')

    item_category_list.extend([
        {
            "MPV_ID": item.get('id'),
            "COUNTRY": locale,
            "SITE_CATEGORY": item.get('parentId'),
            "PARENT_SITE_CATEGORY": category_map.get(item.get('parentId'))
        } for item in response if item.get('type') == 'product'
    ])

# COMMAND ----------

item_category_df = spark.createDataFrame(item_category_list).cache()

# COMMAND ----------

utils.assert_no_duplicates(item_category_df, ["country", "mpv_id"])

# COMMAND ----------

snowflake.table_from_df(
    df=item_category_df,
    database=db,
    schema=schema,
    table="fmx_site_category",
    mode="overwrite")
