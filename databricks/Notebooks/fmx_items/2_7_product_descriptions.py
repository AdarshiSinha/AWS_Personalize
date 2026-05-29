# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import requests
import time

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Get descriptions from marketing-associator API

# COMMAND ----------

snowflake.execute_nonquery(f"""
    CREATE TABLE IF NOT EXISTS {db}.{schema}.fmx_product_mta_descriptions (
        product_key VARCHAR(16777216),
        product_version NUMBER(38,0),
        product_headline VARCHAR(16777216),
        product_description VARCHAR(16777216),
        product_highlights VARCHAR(16777216)
    )
""")

# COMMAND ----------

items_for_mta_query = f"""
    select distinct
        product_key,
        product_version
    from {db}.{schema}.fmx_all_products_raw pr
    where is_active and not exists (
        select 1
        from {db}.{schema}.fmx_product_mta_descriptions pd
        where pd.product_key = pr.product_key and pd.product_version = pr.product_version
    )
"""
items_for_mta_df = snowflake.execute_reader(items_for_mta_query)

# COMMAND ----------

mta_description_list = []
counter = 0
items_for_mta = items_for_mta_df.collect()
status_message = f"products out of {len(items_for_mta)} processed"
for row in items_for_mta:
    product_key = row["PRODUCT_KEY"]
    product_version = int(row["PRODUCT_VERSION"])
    url = f"https://marketing-associator.large-assortment.vpsvc.com/v1/product/{product_key}/version/{product_version}?requestor=precs"
    response = requests.get(url)
    product_description = {
        "product_key": product_key,
        "product_version": product_version,
        "product_headline": None,
        "product_description": None,
        "product_highlights": None
    }
    if response.status_code == 200:
        response_json = response.json()
        if response_json:
            locale_data = response_json["data"]["locales"]
            for locale in ["en-us", "en-ca", "en-gb", "en-ie", "en-au", "en-nz", "en-in", "en-sg"]:
                if locale_data.get(locale):
                    product_description.update({
                        "product_headline": locale_data.get(locale).get("headline"),
                        "product_description": locale_data.get(locale).get("description"),
                        "product_highlights": locale_data.get(locale).get("highlights")
                    })
                    break
    mta_description_list.append(product_description)
    counter += 1
    time.sleep(5)
    if counter % 100 == 0:
        print(f"{counter} {status_message}")
print(f"{counter} {status_message}")

# COMMAND ----------

mta_description_df = spark \
    .createDataFrame(
        mta_description_list,
        schema="product_key string, product_version integer, product_headline string, product_description string, product_highlights string"
    ).select(
        "product_key",
        "product_version",
        "product_headline",
        "product_description",
        "product_highlights"
    ).cache()

# COMMAND ----------

utils.assert_no_duplicates(mta_description_df, ["product_key", "product_version"])

# COMMAND ----------

snowflake.table_from_df(
    df=mta_description_df,
    database=db,
    schema=schema,
    table="fmx_product_mta_descriptions",
    mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get descriptions from merchandising-content-service API

# COMMAND ----------

snowflake.execute_nonquery(f"""
    CREATE TABLE IF NOT EXISTS {db}.{schema}.fmx_product_mcs_descriptions (
        mpv_id VARCHAR(256),
        country VARCHAR(2),
        product_page_title VARCHAR(16777216),
        product_page_description VARCHAR(16777216)
    )
""")

# COMMAND ----------

items_for_mcs_query = f"""
    select distinct
        mpv_id,
        country
    from {db}.{schema}.fmx_all_products_raw pr
    where is_active and not exists (
        select 1
        from {db}.{schema}.fmx_product_mcs_descriptions pd
        where pd.mpv_id = pr.mpv_id and pd.country = pr.country
    )
"""
items_for_mcs_df = snowflake.execute_reader(items_for_mcs_query)

# COMMAND ----------

base_mcs_url = "https://merchandising-content-service-cdn.prod.merch.vpsvc.com/api/v1/vistaprint"
msc_description_list = []
counter = 0
items_for_mcs = items_for_mcs_df.collect()
status_message = f"products out of {len(items_for_mcs)} processed"
for row in items_for_mcs:
    country = row["COUNTRY"]
    culture = utils.region_code[country]
    mpv_id = row["MPV_ID"]
    product_description = {
        "mpv_id": mpv_id,
        "country": country,
        "product_page_title": None,
        "product_page_description": None
    }
    url = f"{base_mcs_url}/types/pageProductPageV2/views/description/entries/{mpv_id}/cultures/{culture}?requestor=precs"
    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json()
        if response_json:
            product_description.update({
                "product_page_title": response_json.get("title"),
                "product_page_description": response_json.get("description")
            })
    msc_description_list.append(product_description)
    counter += 1
    if counter % 100 == 0:
        print(f"{counter} {status_message}")
print(f"{counter} {status_message}")

# COMMAND ----------

mcs_description_df = spark \
    .createDataFrame(
        msc_description_list,
        schema="mpv_id string, country string, product_page_title string, product_page_description string"
    ).select(
        "mpv_id",
        "country",
        "product_page_title",
        "product_page_description"
    ).cache()

# COMMAND ----------

utils.assert_no_duplicates(mcs_description_df, ["mpv_id", "country"])

# COMMAND ----------

snowflake.table_from_df(
    df=mcs_description_df,
    database=db,
    schema=schema,
    table="fmx_product_mcs_descriptions",
    mode="append")
