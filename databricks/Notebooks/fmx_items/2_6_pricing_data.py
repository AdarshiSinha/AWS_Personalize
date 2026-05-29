# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import requests
import json
import urllib.parse

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

region_items = snowflake.execute_reader(f"""
select distinct
  p.product_key,
  p.product_version,
  m.recs_model_region,
  m.recs_primary_locale as country,
from dna.personalization.recs_country_region_mapping m
join {db}.{schema}.fmx_all_products_raw p on
p.country = m.country
where p.is_active
""")

region_country_map = snowflake.execute_reader(f"""
  select distinct
    country,
    recs_model_region
  from dna.personalization.recs_country_region_mapping
""")

# COMMAND ----------

headers = {
    "Authorization": f"Bearer {auth0_token}",
    "Content-Type": "application/json"
}
moq_data = []
for locale, culture in utils.region_code.items():
    url = f"https://poem.personalization.vpsvc.com/v1/offers/vistaprint/{culture}?requestor=precs"
    response = requests.get(url, headers=headers).json()
    for product in response["offers"]:
        moq_data.append({
            "PRODUCT_KEY": product.get("coreProductId"),
            "COUNTRY": locale,
            "OPTIONS": json.dumps(product.get("selectedAttributes", {})),
            "MOQ": product.get("quantitiesSummary").get("moq") if product.get("quantitiesSummary") else 1,
        })

# COMMAND ----------

moq_data_df = spark.createDataFrame(moq_data).dropDuplicates(["PRODUCT_KEY", "COUNTRY"])
items_for_pricing = moq_data_df \
    .join(region_items, ["PRODUCT_KEY", "COUNTRY"]) \
    .select("PRODUCT_KEY", "PRODUCT_VERSION", "COUNTRY", "RECS_MODEL_REGION", "OPTIONS", "MOQ") \
    .distinct() \
    .collect()

# COMMAND ----------

def build_url_string(param_name: str, options: dict) -> str:
    parts = []
    for key, value in options.items():
        part = urllib.parse.quote(f"{param_name}[{key}]") + "=" + urllib.parse.quote(value)
        parts.append(part)
    url_string = '&'.join(parts)
    return url_string

pricing_base_url = 'https://website-pricing-service.prices.cimpress.io/v7/prices?requestor=precs&accountId=ozoDdrmewShEcbUDWX8J3V'
pricing_data = []
counter = 0
status_message = f"product prices out of {len(items_for_pricing)} processed"

for row in items_for_pricing:

    product = row["PRODUCT_KEY"]
    locale = row["COUNTRY"]
    region = row["RECS_MODEL_REGION"]
    version = row["PRODUCT_VERSION"]
    options = json.loads(row["OPTIONS"])
    min_quantity = str(row["MOQ"])
    if options:
        options["Quantity"] = min_quantity
    else:
        options = {"Quantity": min_quantity}
    selections = build_url_string("selections", options)
    context = build_url_string("context", {"Country": locale, "Merchant": "VISTAPRINT"})
    
    try:    
        pricing_url = f"{pricing_base_url}&productId={product}&productVersion={version}&{selections}&{context}"
        pricing_response = requests.get(pricing_url).json()
        fraction_digits = int(pricing_response.get("fractionDigits"))
        currency = pricing_response.get("currency")
        moq_prices = pricing_response.get("prices")[0].get("itemTotals")
        total_moq_price = moq_prices.get("total", {}).get("listUntaxed")/10**fraction_digits
        unit_moq_price = moq_prices.get("unit", {}).get("listUntaxed")/10**fraction_digits
        pricing_data.append({
            "RECS_MODEL_REGION": region,
            "PRODUCT_KEY": product,
            "TOTAL_MOQ_PRICE": total_moq_price,
            "UNIT_MOQ_PRICE": unit_moq_price,
            "CURRENCY": currency,
            "MOQ": min_quantity
        })
    except Exception as e:
        pricing_data.append({
            "RECS_MODEL_REGION": region,
            "PRODUCT_KEY": product,
            "TOTAL_MOQ_PRICE": None,
            "UNIT_MOQ_PRICE": None,
            "CURRENCY": None,
            "MOQ": None
        })

    counter += 1
    if counter % 500 == 0:
        print(f"{counter} {status_message}")
print(f"{counter} {status_message}")

# COMMAND ----------

pricing_data_df = spark \
    .createDataFrame(pricing_data) \
    .join(region_country_map, ["RECS_MODEL_REGION"]) \
    .cache()

# COMMAND ----------

utils.assert_no_duplicates(pricing_data_df, ["country", "product_key"])

# COMMAND ----------

snowflake.table_from_df(
    df=pricing_data_df,
    database=db,
    schema=schema,
    table="fmx_pricing_data",
    mode="overwrite")
