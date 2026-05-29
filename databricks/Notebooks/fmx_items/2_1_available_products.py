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

available_items_url = (
    "https://merchandising-site-experience.prod.merch.vpsvc.com/api/v1/tenant/vistaprint/"
    "culture/{culture}/availability?&requestor=precs"
)
available_items_list = []
for locale, culture in utils.region_code.items():
    url = available_items_url.format(culture=culture)
    response = requests.get(url).json()
    available_items_list.append(
        {
            "country": locale,
            "product_keys": response    
        }
    )

# COMMAND ----------

available_items_df = spark \
    .createDataFrame(available_items_list) \
    .selectExpr(
        "country",
        "explode(product_keys) as product_key",
        "true as msx_available"
    ).cache()

# COMMAND ----------

utils.assert_no_duplicates(available_items_df, ["country", "product_key"])

# COMMAND ----------

snowflake.table_from_df(
    df=available_items_df,
    database=db,
    schema=schema,
    table="fmx_available_products",
    mode="overwrite")
