# Databricks notebook source
artifact_user = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryUser")
artifact_password = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryPwd")
artifactory_url_virtual = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple"

%pip install --index-url {artifactory_url_virtual} vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
    dynamo_users_table = "users_industry_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
    dynamo_users_table = "users_industry_dev"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
    dynamo_users_table = "users_industry_prd"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials(duration=3600*3)
session = utils.get_temp_session(temp_creds)

dynamo = session.resource('dynamodb', region_name="eu-west-1")
users_industry_table = dynamo.Table(dynamo_users_table)

# COMMAND ----------

# we take only users with high confidence - industry_l1_confidence_rank_category less or equal to 8
users_industry_query = """
select
  canonical_id as user_id,
  industry_l1_id as industry_id,
  industry_l1_type as industry_source
from
  vistaprint.shopper.customer_industry_vertical
where
  industry_l1_confidence_rank_category <= 8 and
  canonical_id is not null
"""

# just for the first run - create current table, will be skipped in the next runs
snowflake.execute_nonquery(f"""
create table if not exists {db}.{schema}.ipi_users_industry_current as
{users_industry_query}
""")

# save new users to load to dynamo
snowflake.execute_nonquery(f"""
create or replace table {db}.{schema}.ipi_users_industry_new as
{users_industry_query}
""")

# COMMAND ----------

# collect users to insert to dynamo
users_to_insert = snowflake.execute_reader(f"""
select
  user_id,
  industry_id,
  industry_source
from
  {db}.{schema}.ipi_users_industry_new
""").cache()

# COMMAND ----------

# QA - count check
industry_count_check = users_to_insert.select("industry_id").distinct().count()
if not industry_count_check == 20:
    raise Exception(f"Industry count is not 20, it is {industry_count_check}")

# COMMAND ----------

# QA - null check
null_check = users_to_insert.where("USER_ID is null or INDUSTRY_ID is null or INDUSTRY_SOURCE is null")
if not null_check.isEmpty():
    null_check.display()
    raise Exception("One of the columns is not defined")

# COMMAND ----------

# QA - duplicates check
duplicated_user_check = users_to_insert.groupBy("user_id").count().where("count > 1")
if not duplicated_user_check.isEmpty():
    duplicated_user_check.display()
    raise Exception("There are duplicated users in the dataset")

# COMMAND ----------

# compare new users set with current one
# mark for deletion those users that exist in the current set, but not in the new one
users_to_delete = snowflake.execute_reader(f"""
select old.user_id from {db}.{schema}.ipi_users_industry_current old
where not exists (select 1 from {db}.{schema}.ipi_users_industry_new new where old.user_id = new.user_id)
""").collect()
print(f"{len(users_to_delete)} users will be deleted from DynamoDB")

# COMMAND ----------

with users_industry_table.batch_writer() as batch:
    for row in users_to_delete:
        batch.delete_item(Key={'user_id': row["USER_ID"]})

# COMMAND ----------

users_to_insert = users_to_insert.collect()
print(f"{len(users_to_insert)} users will be inserted to DynamoDB")

# COMMAND ----------

with users_industry_table.batch_writer() as batch:
    for row in users_to_insert:
        batch.put_item(Item={
            'user_id': row["USER_ID"],
            'industry_id': row["INDUSTRY_ID"],
            'industry_source': row["INDUSTRY_SOURCE"]
        })

# COMMAND ----------

# mark the new users set as a current one
snowflake.execute_nonquery(f"""
create or replace table {db}.{schema}.ipi_users_industry_current as
select user_id, industry_id, industry_source from {db}.{schema}.ipi_users_industry_new
""")
