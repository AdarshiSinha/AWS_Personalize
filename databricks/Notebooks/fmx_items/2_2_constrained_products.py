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

active_items_df = snowflake.execute_reader(
    f"select product_key from {db}.{schema}.fmx_all_products_raw where is_active"
).distinct()
active_items_list = [row['PRODUCT_KEY']for row in active_items_df.collect()]
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

constrained_items_df = spark \
    .createDataFrame(constr_items) \
    .where("constraint_attributes = 'all'") \
    .selectExpr(
        "country",
        "product_key",
        "product_version",
        "true as is_constrained") \
    .distinct() \
    .cache()

# COMMAND ----------

utils.assert_no_duplicates(constrained_items_df, ["country", "product_key"])

# COMMAND ----------

snowflake.table_from_df(
    df=constrained_items_df,
    database=db,
    schema=schema,
    table="fmx_constrained_products",
    mode="overwrite")
