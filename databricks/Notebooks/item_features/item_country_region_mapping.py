# Databricks notebook source
artifact_user = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryUser")
artifact_password = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryPwd")
artifactory_url_virtual = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple"

%pip install --index-url {artifactory_url_virtual} vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

if environment == 'dev':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_dev"

elif environment == 'stg':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_stg"

elif environment == 'prd':
    db = "dna"
    output_db = "dna"
    schema = "personalization"

else:
    raise ValueError("Environment is not defined")

print(f"""
Snowflake/Databricks databases and schemas
==========================================
db: {db}
output_db: {output_db}
schema: {schema}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()

# COMMAND ----------

country_region_mapping_query = f"""
create or replace table {db}.{schema}.recs_country_region_mapping as
select country,
case
        when country in ('AU', 'NZ', 'SG') then 'anzs'
        when country in ('DE', 'AT', 'CH') then 'dach'
        when country in ('ES', 'PT', 'IT', 'SE', 'DK', 'NO', 'FI') then 'eu'
        when country in ('FR', 'NL', 'BE') then 'frcen'
        when country in ('GB', 'UK', 'IE') then 'uk'
        else lower(country) --CA, IN, US
    end as recs_model_region,
    case
        recs_model_region
        when 'anzs' then 'AU'
        when 'ca' then 'CA'
        when 'dach' then 'DE'
        when 'eu' then 'IT'
        when 'frcen' then 'FR'
        when 'in' then 'IN'
        when 'uk' then 'GB'
        when 'us' then 'US'
    end as recs_primary_locale,
    sysdate() as created_at
from vistaprint.transactions.dim_country_lob
where country in ('AU', 'NZ', 'SG', 'CA','DE', 'AT', 'CH', 'ES', 'PT', 'IT', 'SE', 'DK'
                    , 'NO', 'FI', 'FR', 'NL', 'BE', 'IN', 'GB', 'UK', 'IE', 'US')
"""

# COMMAND ----------

snowflake.execute_nonquery(country_region_mapping_query)
