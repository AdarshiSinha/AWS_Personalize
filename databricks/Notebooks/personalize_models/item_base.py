# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import requests
import json
import urllib.parse
from pyspark.sql.functions import col, coalesce, lit, current_timestamp, countDistinct
import datetime

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
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
dynamo = session.resource("dynamodb", region_name="eu-west-1")
dynamo_map_table = dynamo.Table(map_table)
snowflake = utils.get_snowflake()
auth0_token = utils.get_auth0_token()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Get MPV - PRD map from MSX

# COMMAND ----------

msx_url = "https://merchandising-site-experience.prod.merch.vpsvc.com/api/v1/tenant/vistaprint/culture/{culture}/mpvs?requestor=precs"
global_map = []
for locale, culture in utils.region_code.items():
    url = msx_url.format(culture=culture)
    response = requests.get(url).json()
    global_map.extend([
        {
            "locale": locale,
            "prd_key": item['productKey'],
            "mpv_id": item['mpvId']
        } for item in response
    ])

# COMMAND ----------

map_df = spark.createDataFrame(global_map)
snowflake.table_from_df(map_df, db, schema, "msx_prd_mpv_map", mode="overwrite")

# COMMAND ----------

with dynamo_map_table.batch_writer() as batch:
    for product in global_map:
        batch.put_item(Item=product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get current items

# COMMAND ----------

active_items = snowflake.execute_reader(f"""
with raw as (
select
  productid,
  productversion,
  status,
  iscurrent,
  eventtimestamp
from
  mcp.marketplace.merchant_product_status_raw
  qualify row_number() over (partition by productid, productversion order by eventtimestamp desc) = 1
),
latest_change as (
  select productid, productversion
  from raw where status = 'ACTIVE' and iscurrent
  qualify row_number() over (partition by productid order by eventtimestamp desc) = 1
)
select
  mpv_id,
  productid as product_key,
  productversion as product_version,
  locale as country
from
  latest_change lc join {db}.{schema}.msx_prd_mpv_map map on
  map.prd_key = lc.productid
""").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Get constrained items

# COMMAND ----------

def puns_check_constraint(product_key):
    url = f"https://product-unavailability.products.cimpress.io/api/v1/products/{product_key}/constraints"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {auth0_token}"
    }

    response = requests.get(url, headers=headers)
    response_json = response.json()

    if response.status_code == 200:
        return response_json.get('results')

    else:
        print(f"Unexpected response from PUNS API: status_code {response.status_code}, response {response.text}")
        return None

# COMMAND ----------

active_items_collect = active_items.select(col("PRODUCT_KEY")).distinct().collect()
active_items_list = [row['PRODUCT_KEY'] for row in active_items_collect]

# COMMAND ----------

constr_items = []

for prd in active_items_list:

    response = puns_check_constraint(prd)
    current_item = []
    if not response:
        continue

    for constr in response:
        context = constr.get('context', [])
        merchant = next((ctx['value'] for ctx in context if ctx.get('key') == 'Merchant'), None)
        country = next((ctx['value'] for ctx in context if ctx.get('key') == 'Country'), None)
        if not merchant or merchant.upper() != 'VISTAPRINT' or not country:
            continue

        constraint_attributes = constr["attributeConstraint"]
        if constraint_attributes is None:
            current_item.append({
                "product_key": constr["productId"],
                "country": country,
                "product_version": constr["productVersion"],
                "constraint_type": constr['type'],
                "constraint_attributes": "all"
            })

        constrained_colors = [
            attr["values"][0]["stringLiteral"]
            for attr in constraint_attributes
            if attr['attributeKey'] == 'Substrate Color'
        ] if constraint_attributes else []

        # Exclude if an 'all' constraint for this product_key and country already exists
        has_all_constraint = any(
            item["product_key"] == constr["productId"] and
            item["country"] == country and
            item["constraint_attributes"] == "all"
            for item in current_item
        )
        if constrained_colors and not has_all_constraint:
            current_item.append({
                "product_key": constr["productId"],
                "country": country,
                "product_version": constr["productVersion"],
                "constraint_type": constr['type'],
                "constraint_attributes": constrained_colors[0]
            })
    constr_items += current_item

# COMMAND ----------

constraint_items_df = spark.createDataFrame(constr_items).distinct().cache()
items_without_constrained = active_items.join(
    constraint_items_df.where("constraint_attributes = 'all'"),
    ["PRODUCT_KEY", "PRODUCT_VERSION", "COUNTRY"],
    "left_anti")
snowflake.table_from_df(constraint_items_df, db, schema, "puns_constraint_items", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Get available items

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
            "COUNTRY": locale,
            "PRODUCT_KEYS": response    
        }
    )
available_items_df = spark.createDataFrame(available_items_list).selectExpr("COUNTRY", "explode(PRODUCT_KEYS) as PRODUCT_KEY")
available_items = items_without_constrained.join(available_items_df, ["COUNTRY", "PRODUCT_KEY"], "left_semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Get site category and MPVs that are products

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
item_category_df = spark.createDataFrame(item_category_list)
active_with_category = available_items.join(item_category_df, ["MPV_ID", "COUNTRY"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Define accessories

# COMMAND ----------

auth_data = json.dumps({
    "grant_type": "client_credentials",
    "client_id": dbutils.secrets.get(scope="personalizationProductRecommender", key="cimpressClientId"),
    "client_secret": dbutils.secrets.get(scope="personalizationProductRecommender", key="cimpressClientSecret"),
    "audience": "https://api.cimpress.io/"
})
access_token = requests.post(
    url="https://cimpress.auth0.com/oauth/token",
    data=auth_data,
    headers={"Content-Type": "application/json"}
).json().get("access_token")
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# COMMAND ----------

product_keys = [p["PRODUCT_KEY"] for p in active_with_category.select("PRODUCT_KEY").distinct().collect()]
accessory_flag_list = []
counter = 0
status_message = f"products out of {len(product_keys)} processed"
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
active_with_accessory_flag = active_with_category.join(accessory_flag_df, "PRODUCT_KEY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Define logomaker enabled items

# COMMAND ----------

response = requests.get("https://logo-matching-products.personalization.vpsvc.com/logo-matching-products.json").json()
logomaker_items = []
for locale, item_list in response.items():
    logomaker_items.append(
        {
            "COUNTRY": locale.upper(),
            "ITEM_LIST": item_list
        }
    )

# COMMAND ----------

logomaker_items_df = spark.createDataFrame(logomaker_items) \
    .selectExpr(
        "COUNTRY",
        "explode(ITEM_LIST) as MPV_ID",
        "1 as IS_LOGOMAKER_ENABLED"
    ).distinct()

active_with_logomaker_flag = active_with_accessory_flag \
    .join(logomaker_items_df, ["MPV_ID", "COUNTRY"], "left") \
    .withColumn("IS_LOGOMAKER_ENABLED", coalesce(col("IS_LOGOMAKER_ENABLED"), lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Get category levels and region

# COMMAND ----------

category_levels = snowflake.execute_reader("""
select product_key, category, subcategory, product_group
from vistaprint.product.product_categorization
where category not in ('Digital', 'Design')
""")

recs_model_region = snowflake.execute_reader("""
select country, recs_model_region, recs_primary_locale
from dna.personalization.recs_country_region_mapping
""")

# COMMAND ----------

items_with_category_levels = active_with_logomaker_flag \
    .join(category_levels, ["PRODUCT_KEY"]) \
    .join(recs_model_region, ["COUNTRY"]) \
    .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Get prices for MOQ

# COMMAND ----------

moq_data = []
for locale, culture in utils.region_code.items():
    url = f"https://poem.personalization.vpsvc.com/v1/offers/vistaprint/{culture}?requestor=precs"
    response = requests.get(url, headers=headers).json()
    for product in response["offers"]:
        moq_data.append({
            "PRODUCT_KEY": product.get("coreProductId"),
            "COUNTRY": locale,
            "OPTIONS": json.dumps(product.get("selectedAttributes", {})),
            "MOQ": product.get("quantitiesSummary").get("moq") if product.get("quantitiesSummary") else 1
        })

# COMMAND ----------

moq_data_df = spark.createDataFrame(moq_data).dropDuplicates(["PRODUCT_KEY", "COUNTRY"])
region_items = items_with_category_levels \
    .selectExpr("PRODUCT_KEY", "PRODUCT_VERSION", "RECS_PRIMARY_LOCALE as COUNTRY", "RECS_MODEL_REGION") \
    .distinct()
all_items = moq_data_df \
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
status_message = f"product prices out of {len(all_items)} processed"

for row in all_items:

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
            "PRODUCT_KEY": product,
            "RECS_MODEL_REGION": region,
            "TOTAL_MOQ_PRICE": total_moq_price,
            "UNIT_MOQ_PRICE": unit_moq_price,
            "CURRENCY": currency,
            "MOQ": min_quantity
        })
    except Exception as e:
        pricing_data.append({
            "PRODUCT_KEY": product,
            "RECS_MODEL_REGION": region,
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

pricing_data_df = spark.createDataFrame(pricing_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Creation timestamp

# COMMAND ----------

merchandise_date_df = snowflake.execute_reader("""
select m.mpv_id,
    crm.country,
    min(n.launch_date) as min_launch_date,
    min(valid_from) as min_mpv_valid_from,
    case
        when coalesce(min_launch_date, '1900-01-01') < min_mpv_valid_from then min_mpv_valid_from
        else min_launch_date
    end as merchandise_date,
    case
        when coalesce(min_launch_date, '1900-01-01') < min_mpv_valid_from then 'mpv_valid_from'
        else 'product_launch'
    end as merchandise_date_source,
    datediff(day, merchandise_date, sysdate()) as days_since_merchandisable,
    DATE_PART(epoch_second, merchandise_date) as CREATION_TIMESTAMP
from vistaprint.product.product_mpv m
    join dna.personalization.recs_country_region_mapping crm on m.country_code = crm.country
    left join vistaprint.product.npi_flag n on m.product_key = n.product_key
    and m.country_code = case
        when n.country_code = 'UK' then 'GB'
        else n.country_code
    end
    and n.merchant = 'VISTAPRINT'
    and product_launch = 1
where m.merchant = 'VISTAPRINT'
group by m.mpv_id,
    crm.country
""")

# COMMAND ----------

base_items = items_with_category_levels \
    .join(pricing_data_df, on=["PRODUCT_KEY", "RECS_MODEL_REGION"], how="left") \
    .join(merchandise_date_df, on=["MPV_ID", "COUNTRY"], how="left") \
    .withColumn("UPDATED_AT", current_timestamp()) \
    .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Quality checks

# COMMAND ----------

# DBTITLE 1,Check for Duplicate MPV IDs by Country
mpv_duplicates = base_items.groupBy("MPV_ID", "COUNTRY").count().filter(col("count") > 1)
if mpv_duplicates.count() > 0:
    mpv_duplicates.display()
    raise Exception("There are duplicate MPV ids in the dataset")

# COMMAND ----------

# DBTITLE 1,Check for Duplicate Product Keys by Country
prd_key_duplicates = base_items.groupBy("PRODUCT_KEY", "COUNTRY").count().filter(col("count") > 1)
if prd_key_duplicates.count() > 0:
    prd_key_duplicates.display()
    raise Exception("There are duplicate PRD keys in the dataset")

# COMMAND ----------

# DBTITLE 1,Check for Null Values in Columns
cols_to_skip = ["SITE_CATEGORY", "PARENT_SITE_CATEGORY", "CURRENCY", "MOQ", "TOTAL_MOQ_PRICE", "UNIT_MOQ_PRICE", "UPDATED_AT", 'MIN_LAUNCH_DATE', 'MIN_MPV_VALID_FROM', 'MERCHANDISE_DATE', 'MERCHANDISE_DATE_SOURCE', 'DAYS_SINCE_MERCHANDISABLE', 'CREATION_TIMESTAMP']
cols_to_check = [col for col in base_items.columns if col not in cols_to_skip]
null_columns = []
for column in cols_to_check:
    if base_items.filter(col(column).isNull()).count() > 0:
        null_columns.append(column)
if null_columns:
    raise Exception(f"Null values found in columns: {null_columns}")

# COMMAND ----------

# DBTITLE 1,Check if pricing data is available for most of the products
total_count = base_items.count()
null_count = base_items.where("TOTAL_MOQ_PRICE is null").count()
null_ratio = round(null_count / total_count, 2)
if null_ratio > 0.05:
    print(f"total_count: {total_count}, null_count: {null_count}, null_ratio: {null_ratio}")
    raise Exception(f"Too many products without pricing data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Save item base to snowflake

# COMMAND ----------

snowflake.table_from_df(
    df=base_items,
    database=db,
    schema=schema,
    table="recommendations_base_items",
    mode="overwrite")
