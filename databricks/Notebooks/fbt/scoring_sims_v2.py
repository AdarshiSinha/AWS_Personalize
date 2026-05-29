# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import io
import boto3 
import pandas as pd
from vp_dna import data_access_layer
from vista_dna_akeyless.akeyless_dna import AKeylessClient
import ast
import json

from pyspark.sql.functions import col, countDistinct, upper, lit, abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, ArrayType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
personalize_runtime_client = utils.get_personalize_runtime_client(temp_creds)
personalize = utils.get_personalize_client(temp_creds)
s3_client = utils.get_s3_client(temp_creds)

# COMMAND ----------

dataset_group_list = [dg["name"] for dg in personalize.list_dataset_groups()["datasetGroups"]]
if environment == "prd":
    dataset_groups_to_update = [dg for dg in dataset_group_list if "test" not in dg]
else:
    dataset_groups_to_update = [dg for dg in dataset_group_list if "test" in dg]

locales = [dg.split('_')[0] for dg in dataset_groups_to_update]
dataset_groups = {locale: dataset_group for locale, dataset_group in zip(locales, dataset_groups_to_update)}

# COMMAND ----------

#Extract the path for the most recent item dataset for each region
latest_users_path = {}
s3_bucket = "precs-product-recommendation"
for locale in locales:
    prefix = f'dna-ppp-product-recommender-data-product/new_pipeline/{dataset_groups[locale]}/items/'
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
    latest_users_path[locale] = latest_file['Key']

# COMMAND ----------

#Extract only accessories and separately items that are not accessories that will
#participate as source products for recommendations retrieval
full_accessory_df = pd.DataFrame(columns=['LOCALE', 'ITEM_ID', 'IS_ACCESSORY'])
df_items_no_accessories = pd.DataFrame(columns=['LOCALE', 'ITEM_ID', 'PRODUCT_GROUP', 'IS_ACCESSORY'])
for i in range(len(locales)):
    obj = s3_client.get_object(Bucket=s3_bucket, Key=latest_users_path[locales[i]])
    items_locale = pd.read_csv(io.BytesIO(obj['Body'].read()))
    # Drop duplicates
    items_locale = items_locale.drop_duplicates(subset=['ITEM_ID'])
    # Create df with local accessory items to append and use later on backup query 
    locale_accessory_df = items_locale[items_locale['IS_ACCESSORY'] == 1][['ITEM_ID', 'IS_ACCESSORY']]
    locale_no_accessory_df = items_locale[items_locale['IS_ACCESSORY'] == 0][['ITEM_ID', 'PRODUCT_GROUP', 'IS_ACCESSORY']]
    locale_accessory_df['LOCALE'] = locales[i].upper()
    locale_no_accessory_df['LOCALE'] = locales[i].upper()   
    full_accessory_df = pd.concat([full_accessory_df,locale_accessory_df], ignore_index=True)
    df_items_no_accessories = pd.concat([df_items_no_accessories,locale_no_accessory_df], ignore_index=True)

# COMMAND ----------

query = '''with mpv_ids as (select country_code, mpv_id, product_key, valid_from, valid_to
from vistaprint.product.product_mpv 
where merchant = 'VISTAPRINT'
and sysdate() between valid_from and valid_to),

order_product_set as (
    select distinct
        upper(crm.RECS_MODEL_REGION) as region,
        lbr.order_number,
        mpv.mpv_id,
        count(distinct mpv.mpv_id) over (partition by crm.RECS_MODEL_REGION, lbr.order_number) as order_size
    from vistaprint.transactions.lob_business_review lbr
    join dna.personalization.recs_country_region_mapping crm on lbr.country = crm.country
    join mpv_ids mpv
    on lbr.product_key = mpv.product_key
    and lbr.country = case when mpv.country_code = 'GB' then 'UK' else mpv.country_code end
    where lbr.order_created_date >= dateadd(month, -15, sysdate()) -- Last 15 months
),
co_purchases as (
    select
        s1.region,
        s1.mpv_id as source_mpv_id,
        s2.mpv_id as combo_mpv_id,
        count(distinct s1.order_number) as order_count, -- Number of co-purchases
        avg(s1.order_size) as avg_order_size
    from order_product_set s1
        join order_product_set s2
            on s1.order_number = s2.order_number
            and s1.mpv_id <> s2.mpv_id
    group by
        s1.region, s1.mpv_id, s2.mpv_id
    having order_count >= 2
),
total_orders as (
    -- Calculate total orders for source product + support(y)
    select
        region,
        mpv_id,
        count(distinct order_number) as distinct_orders,
        ratio_to_report(distinct_orders) over(partition by region) as supp_y
    from order_product_set
    group by region, mpv_id
),
scores as (
    select
        cp.source_mpv_id,
        cp.combo_mpv_id,
        cp.region,
        cp.order_count,
        cp.avg_order_size,
        t_source.distinct_orders,
        cp.order_count * 1.0 / t_source.distinct_orders as confidence, -- Confidence calculation
        t_combo.supp_y,
        (1 - t_combo.supp_y) / (1 - confidence + 0.0000000001) as conviction,
    from co_purchases cp
        join total_orders t_source
            on cp.region = t_source.region
            and cp.source_mpv_id = t_source.mpv_id
        join total_orders t_combo
            on cp.region = t_combo.region
            and cp.combo_mpv_id = t_combo.mpv_id
),
    
rankings as (
    select
        source_mpv_id as mpvid,
        combo_mpv_id as itemlist,
        upper(s.region) as locale,
        distinct_orders,
        0 as distinct_views,
        order_count,
        avg_order_size,
        confidence,
        supp_y,
        conviction,
        count(source_mpv_id) over (partition by s.region, source_mpv_id) as itemlist_count
    from scores s
   qualify itemlist_count >= 5
)

select *
from rankings ra
order by ra.mpvid , LOCALE, CONVICTION desc '''

df2 = snowflake.execute_reader(query)
df_manual_sims = df2.toPandas()

# COMMAND ----------

#Remove accessory products from item list and rank 
df_manual_sims_filt =  pd.merge(df_manual_sims, full_accessory_df, how="left", left_on=["LOCALE", "ITEMLIST"], right_on=["LOCALE", "ITEM_ID"])
df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['IS_ACCESSORY'].isna()]
df_manual_sims_filt['RANK'] = df_manual_sims_filt.\
                                sort_values(['CONVICTION', 'CONFIDENCE', 'SUPP_Y', 'AVG_ORDER_SIZE'], ascending=[False,False,False,True]).\
                                groupby(['LOCALE', 'MPVID']).cumcount() + 1

df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['RANK'] <= 40].sort_values(['LOCALE','MPVID','RANK'], ascending=True)

# COMMAND ----------

assert df_manual_sims_filt['IS_ACCESSORY'].sum() == 0

print(len(df_manual_sims_filt.MPVID.unique()))

# COMMAND ----------

#Remove same merchandising category recommendations
lista_dfs = []
for i in range(len(locales)):
    obj = s3_client.get_object(Bucket=s3_bucket, Key=latest_users_path[locales[i].lower()])
    items_locale = pd.read_csv(io.BytesIO(obj['Body'].read()))
    #extract all recommendations for locale i
    df_filtered = df_manual_sims_filt[df_manual_sims_filt['LOCALE'] == locales[i].upper()]
    #get all merchandising categories for source and recommended products
    merged_df_filtered_1 = df_filtered.merge(items_locale[['ITEM_ID', 'SITE_CATEGORY']], how='left', left_on='MPVID', right_on='ITEM_ID')
    merged_df_filtered_1.rename(columns={'SITE_CATEGORY': 'SITE_CATEGORY_MPVID'}, inplace=True)
    merged_df_filtered_2 = merged_df_filtered_1.merge(items_locale[['ITEM_ID', 'SITE_CATEGORY']], how='left', left_on='ITEMLIST', right_on='ITEM_ID')
    #leave only those recommended products that do not share the same merchandising category
    df_filtered_all = merged_df_filtered_2[merged_df_filtered_2['SITE_CATEGORY_MPVID'] != merged_df_filtered_2['SITE_CATEGORY']]
    #df_filtered_all = df_filtered_all[['MPVID', 'ITEMLIST','LOCALE','RANK']]
    #rerank items after exclusion
    df_filtered_all['RANK'] = df_filtered_all.groupby('MPVID').cumcount() + 1
    lista_dfs.append(df_filtered_all)

df_manual_sims_filt = pd.concat(lista_dfs)
df_manual_sims_filt = df_manual_sims_filt.drop_duplicates().reset_index(drop=True)

# COMMAND ----------

# recheck that item list >= 5 after accessory filter on backup query
df_manual_sims_filt['MAX_RANK'] = df_manual_sims_filt.groupby(['MPVID','LOCALE'])['RANK'].transform('max')
df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['MAX_RANK'] >= 5]
df_manual_sims_filt = df_manual_sims_filt.drop(columns={'MAX_RANK'})

# COMMAND ----------

# Convert the pandas DataFrame to a Spark DataFrame
df_manual_sims_filt_spark = spark.createDataFrame(df_manual_sims_filt[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS','DISTINCT_VIEWS','ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT']])

# COMMAND ----------

#Step 1 check intermediate results after first co-purchases query and accessory filter + merchandising category
current_table_name = 'unique_mpvids_by_locale'

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
    unity_schema = 'dna_e2_dev_productrecommendations_a0ce4e86_2360_460e_8100_81a39afe3cf0'
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
    unity_schema = 'vista_stg_dna_ppp_product_recomme_e481bcfd_9a77_4e0c_90bb_56f5d3644c20'
elif environment == "prd":
    db = "dna"
    schema = "personalization"
    unity_schema = 'vista_prd_dna_ppp_product_recomme_a6f65d5f_cc45_4930_95d5_7b4533446ae7'
else:
    raise Exception("Environment should be dev, stg or prd")


snowflake.table_from_df(df_manual_sims_filt_spark[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS','DISTINCT_VIEWS','ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT']], db, schema, 'SIMS_ITEMS_STEP_1', mode='overwrite')

# COMMAND ----------

print(len(df_manual_sims_filt.MPVID.unique()))

# COMMAND ----------

for i in locales:
    assert len(df_manual_sims_filt[df_manual_sims_filt['LOCALE']==i.upper()].MPVID.unique()) > 0
    print(i, len(df_manual_sims_filt[df_manual_sims_filt['LOCALE']==i.upper()].MPVID.unique()))

# COMMAND ----------

#As a step 2 fallback we will use co-ocurrences based on views and conviction (co-viewed items)
query = '''WITH viewed_product_set AS (
  SELECT DISTINCT
    upper(crm.RECS_MODEL_REGION) as region,
    pv.visit,
    pv.product_id
  FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED pv
  JOIN dna.personalization.recs_country_region_mapping crm on upper(pv.locale) = crm.country
  WHERE pv.ORIGINAL_TIMESTAMP >= DATEADD(month, -15, SYSDATE())
    AND pv.page_section = 'Product Page'
),

co_views AS (
  SELECT
    s1.region,
    s1.product_id AS source_mpv_id,
    s2.product_id AS combo_mpv_id,
    COUNT(DISTINCT s1.visit) AS visit_count
  FROM viewed_product_set s1
  JOIN viewed_product_set s2
    ON s1.visit = s2.visit AND s1.product_id <> s2.product_id
  GROUP BY 1, 2, 3
  HAVING visit_count >= 2
),

total_views AS (
  SELECT
    region,
    product_id AS mpv_id,
    COUNT(DISTINCT visit) AS distinct_visits,
    RATIO_TO_REPORT(COUNT(DISTINCT visit)) OVER (PARTITION BY region) AS supp_y
  FROM viewed_product_set
  GROUP BY region, product_id
),

scores AS (
  SELECT
    cv.source_mpv_id,
    cv.combo_mpv_id,
    cv.region,
    cv.visit_count,
    t_source.distinct_visits,
    cv.visit_count * 1.0 / t_source.distinct_visits AS confidence,
    t_combo.supp_y,
    (1 - t_combo.supp_y) / (1 - (cv.visit_count * 1.0 / t_source.distinct_visits) + 1e-10) AS conviction
  FROM co_views cv
  JOIN total_views t_source
    ON cv.region = t_source.region AND cv.source_mpv_id = t_source.mpv_id
  JOIN total_views t_combo
    ON cv.region = t_combo.region AND cv.combo_mpv_id = t_combo.mpv_id
),

rankings AS (
  SELECT
    s.source_mpv_id AS mpvid,
    s.combo_mpv_id AS itemlist,
    UPPER(s.region) AS locale,
    0 AS distinct_orders,
    s.distinct_visits AS distinct_views,
    s.visit_count AS order_count,
    0 AS avg_order_size,
    s.confidence,
    s.supp_y,
    s.conviction,
    COUNT(*) OVER (PARTITION BY s.region, s.source_mpv_id) AS itemlist_count,
    ROW_NUMBER() OVER (
      PARTITION BY s.region, s.source_mpv_id
      ORDER BY s.conviction DESC, s.confidence DESC, s.supp_y DESC
    ) AS recs_rank
  FROM scores s
  QUALIFY itemlist_count >= 5
)

SELECT *
FROM rankings
WHERE recs_rank <= 50
ORDER BY mpvid, locale, conviction DESC '''

df2 = snowflake.execute_reader(query)
df_viewed_sims = df2.toPandas()

# COMMAND ----------

#Unique items that do have recommendations based on co-purchases and conviction
df_manual_sims_filt_unique = df_manual_sims_filt[['MPVID', 'LOCALE', 'ITEMLIST_COUNT']].drop_duplicates()
#Do left antijoin to leave only products that don't have recommendations
outer = df_items_no_accessories.merge(df_manual_sims_filt_unique, how='outer', left_on=['ITEM_ID', 'LOCALE'], right_on=['MPVID', 'LOCALE'], indicator=True)
df_items_no_recs = outer[(outer._merge=='left_only')].drop('_merge', axis=1)

# COMMAND ----------

#Leave only those MPVIDS that can be found in the item dataset
df_views_to_concat = df_viewed_sims.merge(df_items_no_recs[['LOCALE','ITEM_ID']], how='inner', left_on=['MPVID', 'LOCALE'], right_on=['ITEM_ID', 'LOCALE'])

#Remove accessories from co-views
df_views_to_concat =  pd.merge(df_views_to_concat, full_accessory_df, how="left", left_on=["LOCALE", "ITEMLIST"], right_on=["LOCALE", "ITEM_ID"])
df_views_to_concat = df_views_to_concat[df_views_to_concat['IS_ACCESSORY'].isna()]

#Put the according rank to recommendations limiting to maximum 40
df_views_to_concat['RANK'] = df_views_to_concat.\
                                sort_values(['CONVICTION', 'CONFIDENCE', 'SUPP_Y'], ascending=[False,False,False]).\
                                groupby(['LOCALE', 'MPVID']).cumcount() + 1

df_views_to_concat = df_views_to_concat[df_views_to_concat['RANK'] <= 40].sort_values(['LOCALE','MPVID','RANK'], ascending=True)

# COMMAND ----------

assert df_views_to_concat['IS_ACCESSORY'].sum() == 0

for i in locales:
    assert len(df_views_to_concat[df_views_to_concat['LOCALE']==i.upper()].MPVID.unique()) > 0
    print(i, len(df_views_to_concat[df_views_to_concat['LOCALE']==i.upper()].MPVID.unique()))

# COMMAND ----------

#Concatenate recommendations based on co-purchases and recommendations based on co-views
df_purchased_viewed = pd.concat([df_manual_sims_filt[['MPVID', 'ITEMLIST', 'LOCALE', 'DISTINCT_ORDERS', 'DISTINCT_VIEWS','ORDER_COUNT', 'AVG_ORDER_SIZE', 'CONFIDENCE', 'SUPP_Y', 'CONVICTION', 'ITEMLIST_COUNT', 'RANK']], df_views_to_concat[['MPVID', 'ITEMLIST', 'LOCALE','DISTINCT_ORDERS','DISTINCT_VIEWS', 'ORDER_COUNT', 'AVG_ORDER_SIZE', 'CONFIDENCE', 'SUPP_Y', 'CONVICTION', 'ITEMLIST_COUNT', 'RANK']]])

# Convert the pandas DataFrame to a Spark DataFrame
df_purchased_viewed_spark = spark.createDataFrame(df_purchased_viewed[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS','DISTINCT_VIEWS','ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT']])

# COMMAND ----------

#Step 2 check intermediate results after adding co-views

snowflake.table_from_df(df_purchased_viewed_spark[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS','DISTINCT_VIEWS','ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT']], db, schema, 'SIMS_ITEMS_STEP_2', mode='overwrite')

# COMMAND ----------

#Do left antijoin to leave only products that don't have recommendations
df_purchased_viewed_unique = df_purchased_viewed[['MPVID', 'LOCALE', 'ITEMLIST_COUNT']].drop_duplicates()
outer = df_items_no_accessories.merge(df_purchased_viewed_unique, how='outer', left_on=['ITEM_ID', 'LOCALE'], right_on=['MPVID', 'LOCALE'], indicator=True)
df_items_no_recs = outer[(outer._merge=='left_only')].drop('_merge', axis=1)

# COMMAND ----------

#Attach Product Group information to dfs
df_purchased_unique_product_group = df_manual_sims_filt_unique.merge(df_items_no_accessories[['LOCALE', 'ITEM_ID', 'PRODUCT_GROUP']], how='left', left_on=['MPVID', 'LOCALE'], right_on=['ITEM_ID', 'LOCALE'])
df_viewed_unique_product_group = df_purchased_viewed_unique.merge(df_items_no_accessories[['LOCALE', 'ITEM_ID', 'PRODUCT_GROUP']], how='left', left_on=['MPVID', 'LOCALE'], right_on=['ITEM_ID', 'LOCALE'])

# COMMAND ----------

#For every source item that does not have recommendations
#Find an item within the same product group that has the highest number of recommendations
#Map those recommendations to the source item
df_list = []
for row in df_items_no_recs.itertuples(index=False):
    product_group, locale, item = row.PRODUCT_GROUP, row.LOCALE, row.ITEM_ID
    
    df_filtered = df_purchased_unique_product_group.query(
        "PRODUCT_GROUP == @product_group and LOCALE == @locale"
    ).copy()

    #if there is no item within the same product group based on co-purchases
    #try with co-views
    if df_filtered.empty:
        df_filtered = df_viewed_unique_product_group.query(
        "PRODUCT_GROUP == @product_group and LOCALE == @locale"
    ).copy()

    if df_filtered.empty:
        continue   

    df_filtered["ITEMLIST_COUNT"] = pd.to_numeric(df_filtered["ITEMLIST_COUNT"], errors="coerce")
    df_filtered = df_filtered.nlargest(1, 'ITEMLIST_COUNT')
    new_item = df_filtered.iloc[0]['MPVID']
    
    df_result = df_purchased_viewed.query(
        "MPVID == @new_item and LOCALE == @locale"
    ).replace({'MPVID': {new_item: item}})
    
    df_list.append(df_result)

df_replaced_mpvids_by_product_group = pd.concat(df_list, ignore_index=True)

# COMMAND ----------

#Concatenate recommendations based on co-purchases and co-views and similar item recommendations
df_purchased_viewed_similar = pd.concat([df_purchased_viewed[['MPVID', 'ITEMLIST', 'LOCALE', 'DISTINCT_ORDERS', 'DISTINCT_VIEWS', 'ORDER_COUNT', 'AVG_ORDER_SIZE', 'CONFIDENCE', 'SUPP_Y', 'CONVICTION', 'ITEMLIST_COUNT', 'RANK']], df_replaced_mpvids_by_product_group[['MPVID', 'ITEMLIST', 'LOCALE', 'DISTINCT_ORDERS', 'DISTINCT_VIEWS', 'ORDER_COUNT', 'AVG_ORDER_SIZE', 'CONFIDENCE', 'SUPP_Y', 'CONVICTION', 'ITEMLIST_COUNT', 'RANK']]])

# Convert the pandas DataFrame to a Spark DataFrame
df_purchased_viewed_similar_spark = spark.createDataFrame(df_purchased_viewed_similar[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS', 'DISTINCT_VIEWS', 'ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT','RANK']])

# COMMAND ----------

df_us = df_purchased_viewed_similar_spark.filter(F.col("LOCALE") == "US").cache()

#Compute top 25 MPVIDs by DISTINCT_ORDERS
top_25_mpvids = (
    df_us.groupBy("MPVID")
    .agg(F.max("DISTINCT_ORDERS").alias("DISTINCT_ORDERS"))
    .orderBy(F.desc("DISTINCT_ORDERS"))
    .limit(25)
    .cache()
)


df_top = df_us.join(top_25_mpvids.select("MPVID"), on="MPVID", how="inner")

df_final = df_top.filter(F.col("rank").between(1, 10))

df_min_order_sorted = (
    df_final.groupBy("MPVID")
    .agg(
        F.min("ORDER_COUNT").alias("MIN_ORDER_COUNT"),
        F.max("DISTINCT_ORDERS").alias("DISTINCT_ORDERS")
    )
    .orderBy(F.desc("DISTINCT_ORDERS"))
)

#All MIN_ORDER_COUNT values must be > 100
min_order_count = df_min_order_sorted.agg(F.min("MIN_ORDER_COUNT")).collect()[0][0]
assert min_order_count > 100, f"Minimum order count is too low: {min_order_count}"


# COMMAND ----------

#Step 3 check intermediate results after adding similar product group recommendations

snowflake.table_from_df(df_purchased_viewed_similar_spark[['MPVID','ITEMLIST','LOCALE','DISTINCT_ORDERS','ORDER_COUNT','AVG_ORDER_SIZE','CONFIDENCE','SUPP_Y','CONVICTION','ITEMLIST_COUNT']], db, schema, 'SIMS_ITEMS_STEP_3', mode='overwrite')

# COMMAND ----------

#Do left antijoin to leave only products that don't have recommendations
df_purchased_viewed_similar_unique = df_purchased_viewed_similar[['MPVID', 'LOCALE', 'ITEMLIST_COUNT']].drop_duplicates()
outer = df_items_no_accessories.merge(df_purchased_viewed_similar_unique, how='outer', left_on=['ITEM_ID', 'LOCALE'], right_on=['MPVID', 'LOCALE'], indicator=True)
df_items_no_recs = outer[(outer._merge=='left_only')].drop('_merge', axis=1)

# COMMAND ----------

print(len(df_items_no_recs[~df_items_no_recs['PRODUCT_GROUP'].isin(['Social Media', 'DIWY','Digital Other', 'Wix Other','Websites & Domains'])]))

# COMMAND ----------

print(len(df_purchased_viewed_similar.MPVID.unique()))

# COMMAND ----------

#Remove same merchandising category recommendations
lista_dfs = []
for i in range(len(locales)):
    obj = s3_client.get_object(Bucket=s3_bucket, Key=latest_users_path[locales[i].lower()])
    items_locale = pd.read_csv(io.BytesIO(obj['Body'].read()))
    #extract all recommendations for locale i
    df_filtered = df_purchased_viewed_similar[df_purchased_viewed_similar['LOCALE'] == locales[i].upper()]
    #get all merchandising categories for source and recommended products
    merged_df_filtered_1 = df_filtered.merge(items_locale[['ITEM_ID', 'SITE_CATEGORY']], how='left', left_on='MPVID', right_on='ITEM_ID')
    merged_df_filtered_1.rename(columns={'SITE_CATEGORY': 'SITE_CATEGORY_MPVID'}, inplace=True)
    merged_df_filtered_2 = merged_df_filtered_1.merge(items_locale[['ITEM_ID', 'SITE_CATEGORY']], how='left', left_on='ITEMLIST', right_on='ITEM_ID')
    #leave only those recommended products that do not share the same merchandising category
    df_filtered_all = merged_df_filtered_2[merged_df_filtered_2['SITE_CATEGORY_MPVID'] != merged_df_filtered_2['SITE_CATEGORY']]
    df_filtered_all = df_filtered_all[['MPVID', 'ITEMLIST','LOCALE','RANK']]
    #rerank items after exclusion
    df_filtered_all['RANK'] = df_filtered_all.groupby('MPVID').cumcount() + 1
    lista_dfs.append(df_filtered_all)

df_filtered_all = pd.concat(lista_dfs)
df_filtered_all = df_filtered_all.drop_duplicates().reset_index(drop=True)

# COMMAND ----------

print(len(df_filtered_all.MPVID.unique()))

# COMMAND ----------

# recheck that item list >= 5 after merchandising category filter on backup query
df_filtered_all['MAX_RANK'] = df_filtered_all.groupby(['MPVID','LOCALE'])['RANK'].transform('max')
df_filtered_all = df_filtered_all[df_filtered_all['MAX_RANK'] >= 5]
df_filtered_all = df_filtered_all.drop(columns={'MAX_RANK'})

# COMMAND ----------

# Convert the pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(df_filtered_all)

# Show the Spark DataFrame
spark_df.display()

# COMMAND ----------

locales_upper = [loc.upper() for loc in locales]

result_df = spark_df.groupBy("LOCALE") \
    .agg(countDistinct("MPVID").alias("UNIQUE_MPVIDS"))

result_df.orderBy("LOCALE").show()

# COMMAND ----------

#QA on variability in the old and the new number of unique MPVIDs by locale
#Try to load existing Delta table (previous counts)
try:
    delta_table_path = f"vista.{unity_schema}.{current_table_name}_counts"
    delta_df = spark.read.format("delta").table(delta_table_path)
    print('Delta table exists. Reading previous unique MPVID counts')

    joined_df = result_df.alias("new").join(
        delta_df.alias("old"),
        on="LOCALE",
        how="inner"
    ).select(
        col("LOCALE"),
        col("new.UNIQUE_MPVIDS").alias("new_count"),
        col("old.UNIQUE_MPVIDS").alias("old_count"),
        ((col("new.UNIQUE_MPVIDS") - col("old.UNIQUE_MPVIDS")) / col("old.UNIQUE_MPVIDS")).alias("relative_change")
    )
    display(joined_df)

    large_changes = joined_df.filter(abs(col("relative_change")) > 0.1)

    if large_changes.count() > 0:
        large_changes.show()
        raise ValueError("Detected >10% change in unique MPVIDs for some locales.")
    else:
        print("No large variation detected. Proceeding to store new counts.")

except AnalysisException:
    print("Delta table does not exist. Proceeding to create it.")

result_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"vista.{unity_schema}.{current_table_name}_counts")

print("Stored current unique MPVID counts into Delta table.")

# COMMAND ----------

snowflake.table_from_df(spark_df, db, schema, 'SIMS_ITEMS_V2_TEMP', mode='overwrite')

# COMMAND ----------

fbt_items_v2_dynamo = snowflake.execute_reader(f"""

with
country_sims_items as (
select distinct
  mpvid,
  itemlist,
  rank,
  locale_value.value::string as country_code
from (
  select
    mpvid,
    itemlist,
    rank,
    CASE
      WHEN locale = 'FRCEN' THEN ['FR', 'BE', 'NL']
      WHEN locale = 'ANZS' THEN ['AU', 'NZ', 'SG']
      WHEN locale = 'DACH' THEN ['DE', 'AT', 'CH']
      WHEN locale = 'EU' THEN ['ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO']
      WHEN locale = 'UK' THEN ['GB', 'IE']
      WHEN locale = 'US' THEN ['US']
      WHEN locale = 'CA' THEN ['CA']
      WHEN locale = 'IN' THEN ['IN']
    END AS locale_list
  from {db}.{schema}.SIMS_ITEMS_V2_TEMP
) AS base,
lateral flatten(input => base.locale_list) as locale_value
),
clean_sims_items as (
  select mpvid as item_id, itemlist, rank, country_code as locale
  from country_sims_items
  where (country_code, itemlist) in  (select country, mpv_id from DNA.PERSONALIZATION.RECOMMENDATIONS_BASE_ITEMS)
)
select
  item_id,
  locale,
  listagg(itemlist, ',') within group (order by rank) as fbt_items_v2
from
  clean_sims_items
group by
  item_id,
  locale
having count(itemlist) >= 5

""").cache()

distinct_items = fbt_items_v2_dynamo.select("locale", "item_id").distinct().count()
all_items = fbt_items_v2_dynamo.count()
if distinct_items != all_items:
    fbt_items_v2_dynamo.groupBy("locale", "item_id").count().where("count > 1").display()
    raise Exception(f"There are duplicated records in the final dataset")

distinct_locales = fbt_items_v2_dynamo.select("locale").distinct().count()
if distinct_locales != 21:
    fbt_items_v2_dynamo.select("locale").display()
    raise Exception(f"There should be exactly 21 locales, got {distinct_locales}")

# COMMAND ----------

fbt_items_v2_sf = snowflake.execute_reader(f"""

with
country_sims_items as (
select distinct
  mpvid,
  itemlist,
  rank,
  locale_value.value::string as country_code
from (
  select
    mpvid,
    itemlist,
    rank,
    CASE
      WHEN locale = 'FRCEN' THEN ['FR', 'BE', 'NL']
      WHEN locale = 'ANZS' THEN ['AU', 'NZ', 'SG']
      WHEN locale = 'DACH' THEN ['DE', 'AT', 'CH']
      WHEN locale = 'EU' THEN ['ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO']
      WHEN locale = 'UK' THEN ['GB', 'IE']
      WHEN locale = 'US' THEN ['US']
      WHEN locale = 'CA' THEN ['CA']
      WHEN locale = 'IN' THEN ['IN']
    END AS locale_list
  from {db}.{schema}.SIMS_ITEMS_V2_TEMP
) AS base,
lateral flatten(input => base.locale_list) as locale_value
),
clean_sims_items as (
  select mpvid, itemlist, rank, country_code as locale, count(itemlist) over(partition by mpvid, country_code) as item_list_count
  from country_sims_items
  where (country_code, itemlist) in  (select country, mpv_id from DNA.PERSONALIZATION.RECOMMENDATIONS_BASE_ITEMS)
  qualify item_list_count >= 5
)
select
  mpvid,
  itemlist,
  locale,
  row_number() over(partition by mpvid, locale order by rank asc) as rank
from
  clean_sims_items
""").cache()

# COMMAND ----------

snowflake.table_from_df(fbt_items_v2_sf, db, schema, 'SIMS_ITEMS_V2', mode='overwrite')

# COMMAND ----------

dynamo = session.resource('dynamodb', region_name="eu-west-1")

fbt_dynamo_table = dynamo.Table(
    "frequently_bought_together_v2_prd" if environment == "prd" else "frequently_bought_together_v2_dev"
)

# COMMAND ----------

# In order to overwrite the whole dynamo table we need to:
# 1. read all items from it
# 2. identify and delete those items that are not in the dataframe we want to write (left anti join)
# 3. write items from the dataframe to dynamo

scan_response = fbt_dynamo_table.scan()
all_dynamo_items = scan_response['Items']
while scan_response.get("LastEvaluatedKey"):
    scan_response = fbt_dynamo_table.scan(ExclusiveStartKey=scan_response['LastEvaluatedKey'])
    all_dynamo_items.extend(scan_response['Items'])

current_items = sc.parallelize(all_dynamo_items).toDF().selectExpr("item_id", "locale", "items as fbt_items_v2")
fbt_items_v2_dynamo = fbt_items_v2_dynamo.toDF(*[c.lower() for c in fbt_items_v2_dynamo.columns])

items_to_delete = current_items \
    .join(fbt_items_v2_dynamo, ["item_id", "locale"], "left_anti") \
    .select("item_id", "locale") \
    .collect()

with fbt_dynamo_table.batch_writer() as batch:
    for row in items_to_delete:
        batch.delete_item(Key={'item_id': row["item_id"], 'locale': row["locale"]})

# COMMAND ----------

with fbt_dynamo_table.batch_writer() as batch:
    for row in fbt_items_v2_dynamo.collect():
        batch.put_item(Item={
            "item_id": row["item_id"],
            "locale": row["locale"],
            "items": row["fbt_items_v2"]
        })
