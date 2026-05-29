# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

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

merchandise_date_df = snowflake.execute_reader(f"""
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
""").cache()

# COMMAND ----------

utils.assert_no_duplicates(merchandise_date_df, ["country", "mpv_id"])

# COMMAND ----------

snowflake.table_from_df(
    df=merchandise_date_df,
    database=db,
    schema=schema,
    table="fmx_merchandise_dates",
    mode="overwrite")
