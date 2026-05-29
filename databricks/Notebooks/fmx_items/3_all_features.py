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

snowflake.execute_nonquery(f"""
create or replace table {db}.{schema}.fmx_all_products as
with high_value_proxy as (
    select
        hvp.mpv_id,
        crm.country,
        hvp.high_value_proxy_category
    from
        dna.personalization.product_high_value_proxy hvp join dna.personalization.recs_country_region_mapping crm on
        hvp.recs_model_region = crm.recs_model_region
),
product_bulk_proxy as (
    select
        bp.mpv_id,
        crm.country,
        bp.bulk_proxy_category,
        bp.product_median_quantity
    from
        dna.personalization.product_bulk_proxy bp join dna.personalization.recs_country_region_mapping crm on
        bp.recs_model_region = crm.recs_model_region
),
product_ratings_proxy as (
    select
        rpc.mpv_id,
        crm.country,
        rpc.calc_product_rating,
        rpc.rating_proxy_category
    from
        dna.personalization.product_ratings_proxy rpc join dna.personalization.recs_country_region_mapping crm on
        rpc.recs_model_region = crm.recs_model_region
),
design_complexity_proxy as (
    select
        dcp.mpv_id,
        crm.country,
        dcp.calc_studio_duration,
        dcp.design_complexity_proxy_category
    from
        dna.personalization.product_design_complexity_proxy dcp join dna.personalization.recs_country_region_mapping crm on
        dcp.recs_model_region = crm.recs_model_region
)
select
    ap.product_key,
    ap.product_version,
    ap.mpv_id,
    ap.country,
    ap.priority,
    ap.is_active,
    ap.category,
    ap.subcategory,
    ap.product_group,
    sc.parent_site_category,
    sc.site_category,
    mta.product_description,
    mta.product_headline,
    mta.product_highlights,
    mcs.product_page_title,
    mcs.product_page_description,
    pd.currency,
    pd.moq,
    pd.total_moq_price,
    pd.unit_moq_price,
    md.min_launch_date,
    md.min_mpv_valid_from,
    md.merchandise_date,
    md.merchandise_date_source,
    md.days_since_merchandisable,
    md.creation_timestamp,
    ifnull(lf.logomaker_enabled, 0) as logomaker_enabled,
    af.is_accessory,
    ifnull(cpr.is_constrained, false) as is_constrained,
    ifnull(apr.msx_available, false) as is_available,
    hvp.high_value_proxy_category,
    pbp.bulk_proxy_category,
    pbp.product_median_quantity,
    prp.calc_product_rating,
    prp.rating_proxy_category,
    dcp.calc_studio_duration,
    dcp.design_complexity_proxy_category
from
    {db}.{schema}.fmx_all_products_raw ap left join
    {db}.{schema}.fmx_site_category sc on ap.mpv_id = sc.mpv_id and ap.country = sc.country left join
    {db}.{schema}.fmx_product_mta_descriptions mta on ap.product_key = mta.product_key and ap.product_version = mta.product_version left join
    {db}.{schema}.fmx_product_mcs_descriptions mcs on ap.mpv_id = mcs.mpv_id and ap.country = mcs.country left join
    {db}.{schema}.fmx_pricing_data pd on ap.product_key = pd.product_key and ap.country = pd.country left join
    {db}.{schema}.fmx_merchandise_dates md on ap.mpv_id = md.mpv_id and ap.country = md.country left join
    {db}.{schema}.fmx_logomaker_flags lf on ap.mpv_id = lf.mpv_id and ap.country = lf.country left join
    {db}.{schema}.fmx_accessory_flags af on ap.product_key = af.product_key left join
    {db}.{schema}.fmx_constrained_products cpr on ap.product_key = cpr.product_key and ap.product_version = cpr.product_version and ap.country = cpr.country left join
    {db}.{schema}.fmx_available_products apr on ap.product_key = apr.product_key and ap.country = apr.country left join
    high_value_proxy hvp on ap.mpv_id = hvp.mpv_id and ap.country = hvp.country left join
    product_bulk_proxy pbp on ap.mpv_id = pbp.mpv_id and ap.country = pbp.country left join
    product_ratings_proxy prp on ap.mpv_id = prp.mpv_id and ap.country = prp.country left join
    design_complexity_proxy dcp on ap.mpv_id = dcp.mpv_id and ap.country = dcp.country
""")
