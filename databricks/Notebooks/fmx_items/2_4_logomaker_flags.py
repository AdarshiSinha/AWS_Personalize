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

response = requests.get("https://logo-matching-products.personalization.vpsvc.com/logo-matching-products.json").json()
logomaker_items = []
for locale, item_list in response.items():
    logomaker_items.append(
        {
            "country": locale.upper(),
            "item_list": item_list
        }
    )

# COMMAND ----------

logomaker_items_df = spark.createDataFrame(logomaker_items) \
    .selectExpr(
        "country",
        "explode(item_list) as mpv_id",
        "1 as logomaker_enabled"
    ).groupBy(
        "country",
        "mpv_id"
    ).agg(
        f.max("logomaker_enabled").alias("logomaker_enabled")
    ).cache()

# COMMAND ----------

utils.assert_no_duplicates(logomaker_items_df, ["country", "mpv_id"])

# COMMAND ----------

snowflake.table_from_df(
    df=logomaker_items_df,
    database=db,
    schema=schema,
    table="fmx_logomaker_flags",
    mode="overwrite")
