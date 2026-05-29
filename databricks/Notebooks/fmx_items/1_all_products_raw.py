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
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
dynamo = session.resource("dynamodb", region_name="eu-west-1")
dynamo_map_table = dynamo.Table(map_table)
snowflake = utils.get_snowflake()

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

# dynamo mpv-prd map is used in segment connector to covert prd keys in events to mpv ids
with dynamo_map_table.batch_writer() as batch:
    for product in global_map:
        batch.put_item(Item=product)

# COMMAND ----------

all_with_active_df = snowflake.execute_reader(f"""
with all_prds as (
    select
        locale as country,
        mpv_id,
        prd_key as product_key,
        1 as priority
    from {db}.{schema}.msx_prd_mpv_map
    union all
    select
        country_code as country,
        mpv_id,
        product_key,
        2 as priority
    from vistaprint.product.product_mpv
    where tenant_website = 'VP'
    qualify row_number() over (
        partition by country_code, product_key
        order by valid_from desc, created_at desc
    ) = 1
),
all_prds_versions as (
    select
        country,
        mpv_id,
        product_key,
        priority
    from all_prds
    qualify row_number() over (partition by country, product_key order by priority asc) = 1
),
raw_active_items as (
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
    from raw_active_items where status = 'ACTIVE' and iscurrent
    qualify row_number() over (partition by productid order by eventtimestamp desc) = 1
),
active_items as (
    select
      mpv_id,
      productid as product_key,
      productversion as product_version,
      locale as country
    from
      latest_change lc join {db}.{schema}.msx_prd_mpv_map map on
      map.prd_key = lc.productid
)
select
    av.country,
    av.product_key,
    av.mpv_id,
    ai.product_version,
    pc.category,
    pc.subcategory,
    pc.product_group,
    av.priority,
    ifnull(ai.product_version, false) as is_active
from all_prds_versions av
left join active_items ai on
    av.product_key = ai.product_key and
    av.mpv_id = ai.mpv_id and
    av.country = ai.country
left join vistaprint.product.product_categorization pc on
    av.product_key = pc.product_key
""").cache()

# COMMAND ----------

utils.assert_no_duplicates(all_with_active_df.where("is_active"), ["country", "product_key"])
utils.assert_no_duplicates(all_with_active_df.where("is_active"), ["country", "mpv_id"])

# COMMAND ----------

snowflake.table_from_df(
    df=all_with_active_df,
    database=db,
    schema=schema,
    table="fmx_all_products_raw",
    mode="overwrite")
