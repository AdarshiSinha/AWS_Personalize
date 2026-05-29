# Databricks notebook source
artifact_user = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryUser")
artifact_password = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryPwd")

artifactory_url_virtual = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple"
artifactory_url_local = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-local/simple"

%pip install --index-url {artifactory_url_virtual} vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("full_refresh", "0")
full_refresh = dbutils.widgets.get("full_refresh")

# COMMAND ----------

if environment == 'dev':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_dev"
    akeyless_access_id = "p-60q2bjh345fy"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'dna_e2_dev_productrecommendations_a0ce4e86_2360_460e_8100_81a39afe3cf0'
    current_embedding_table_name = 'ps_config_embeddings'

elif environment == 'stg':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_stg"
    akeyless_access_id = "p-oium5k724hbj"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'vista_stg_dna_ppp_product_recomme_e481bcfd_9a77_4e0c_90bb_56f5d3644c20'
    current_embedding_table_name = 'ps_config_embeddings'

elif environment == 'prd':
    db = "dna"
    output_db = "dna"
    schema = "personalization"
    akeyless_access_id = "p-hbnpveay8mtx"
    substitution_items_dynamo_table_name = "substitution_items_prd"
    unity_schema = 'vista_prd_dna_ppp_product_recomme_a6f65d5f_cc45_4930_95d5_7b4533446ae7'
    current_embedding_table_name = 'ps_config_embeddings'
    
else:
    raise ValueError("Environment is not defined")

print(f"""
Snowflake/Databricks databases and schemas
==========================================
db: {db}
output_db: {output_db}
schema: {schema}
akeyless_access_id: {akeyless_access_id}
substitution_items_dynamo_table_name: {substitution_items_dynamo_table_name}
current_embedding_table_name: {current_embedding_table_name}
full_refresh: {full_refresh}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()

# COMMAND ----------

if full_refresh == '1':
    drop_statement = f"""
    DROP TABLE IF EXISTS IDENTIFIER('vista.{unity_schema}.ps_config_embeddings')
    """

    print(drop_statement)

    
    spark.sql(drop_statement)
