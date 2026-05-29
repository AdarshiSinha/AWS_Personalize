# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

import io
import boto3 
import pandas as pd
from vp_dna import data_access_layer
from vista_dna_akeyless.akeyless_dna import AKeylessClient
import ast
import json
import numpy as np

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

def get_snowflake(environment):
    access_ids = {
        "dev": "p-60q2bjh345fy",
        "stg": "p-oium5k724hbj",
        "prd": "p-hbnpveay8mtx",
    }
    akeyless_client = AKeylessClient(access_id=access_ids[environment])
    sf_secret_path = f"/vistaprint/dna/ppp/team/precs/snowflake-product-recommendations-{environment}"
    akeyless_response = akeyless_client.get_secret_from_full_path(secret_path=sf_secret_path)
    snowflake_creds = ast.literal_eval(akeyless_response['snowflake'])
    client_id = snowflake_creds['username']
    client_secret = snowflake_creds['password']
    client_role = snowflake_creds['username'].split("_")[1]

    return data_access_layer.Snowflake(
        client_id=client_id,
        client_secret=client_secret,
        client_role=client_role,
        spark=spark,
        default_warehouse="RECOMMENDATIONS_DNA_WH",
        credentials_type='SNOWFLAKE',
        query_tag=json.dumps({
            "domain_name": "Marketing Optimization",
            "product_team_name": "CTAP",
            "data_product_name": "recommendations",
            "databricks_product_tag": "recommendations",
            "target_name": environment
        }))

snowflake = get_snowflake(environment)

# COMMAND ----------

aws_role_session = "scoring_sims"
aws_personalize_role = dbutils.secrets.get(
    scope="personalizationProductRecommender",
    key="awsPersonalizeAccountRole")
client = boto3.client('sts')
StsResponse = client.assume_role(RoleArn=aws_personalize_role, RoleSessionName=aws_role_session)

# COMMAND ----------

latest_users_path = {}
campaign_arn = {
    'us': 'arn:aws:personalize:eu-west-1:905666450942:campaign/us_20240523153317_v4_sims_sol_cpn',
    'frcen': 'arn:aws:personalize:eu-west-1:905666450942:campaign/frcen_20240411104415_v4_sims_sol_cpn',
    'uk': 'arn:aws:personalize:eu-west-1:905666450942:campaign/uk_20240410185804_v4_sims_sol_cpn',
    'ca': 'arn:aws:personalize:eu-west-1:905666450942:campaign/ca_20240523153027_v4_sims_sol_cpn',
    'anzs': 'arn:aws:personalize:eu-west-1:905666450942:campaign/anzs_20240410142828_v4_sims_sol_cpn',
    'eu': 'arn:aws:personalize:eu-west-1:905666450942:campaign/eu_20240411105111_v4_sims_sol_cpn',
    'dach': 'arn:aws:personalize:eu-west-1:905666450942:campaign/dach_20240410222906_v4_sims_sol_cpn',
    'in': 'arn:aws:personalize:eu-west-1:905666450942:campaign/in_20240522122520_v4_sims_sol_cpn'
}
accessory_filter_arn = {
    'us': 'arn:aws:personalize:eu-west-1:905666450942:filter/us_20240523153317_v4_accessory_filter',
    'frcen': 'arn:aws:personalize:eu-west-1:905666450942:filter/frcen_20240411104415_v4_accessory_filter',
    'uk': 'arn:aws:personalize:eu-west-1:905666450942:filter/uk_20240410185804_v4_accessory_filter',
    'ca': 'arn:aws:personalize:eu-west-1:905666450942:filter/ca_20240523153027_v4_accessory_filter',
    'anzs': 'arn:aws:personalize:eu-west-1:905666450942:filter/anzs_20240410142828_v4_accessory_filter',
    'eu': 'arn:aws:personalize:eu-west-1:905666450942:filter/eu_20240411105111_v4_accessory_filter',
    'dach': 'arn:aws:personalize:eu-west-1:905666450942:filter/dach_20240410222906_v4_accessory_filter',
    'in': 'arn:aws:personalize:eu-west-1:905666450942:filter/in_20240522122520_v4_accessory_filter'
}
#locales = list(campaign_arn.keys())
locales = ['ca'] #['us','ca','dach','frcen','uk']
version = "v4"
item_list_min = 3

# COMMAND ----------

personalize_runtime_client = boto3.client(
    'personalize-runtime',
    region_name='eu-west-1',
    aws_access_key_id=StsResponse['Credentials']['AccessKeyId'],
    aws_secret_access_key=StsResponse['Credentials']['SecretAccessKey'],
    aws_session_token=StsResponse['Credentials']['SessionToken'])

# COMMAND ----------

s3_client = boto3.client(
    's3',
    aws_access_key_id=StsResponse['Credentials']['AccessKeyId'],
    aws_secret_access_key=StsResponse['Credentials']['SecretAccessKey'],
    aws_session_token=StsResponse['Credentials']['SessionToken'])

# COMMAND ----------

s3_bucket = "precs-product-recommendation"
for locale in locales:
    prefix = f'dna-ppp-product-recommender-data-product/pipeline/{locale}/{version}/features/'
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix, Delimiter='/')
    # Extract folder names
    folders = [obj['Prefix'] for obj in response['CommonPrefixes']]
    # Sort folders by their name
    folders.sort(reverse=True)
    s3_path = folders[0] + 'item_level/item_level_features.csv'
    latest_users_path[locale] = s3_path

# COMMAND ----------

# Function to fetch recommendations and check if they match the criteria
def fetch_and_check_recommendations(item_id, campaign_arn, filter_arn):
    recommendations = personalize_runtime_client.get_recommendations(campaignArn=campaign_arn, filterArn=filter_arn, itemId=item_id, numResults = 30)
    return item_id, [d['itemId'] for d in recommendations["itemList"]]

def check_default(item_lista, default_list):
    return 1 if default_list in item_lista else 0

# COMMAND ----------

obj = s3_client.get_object(Bucket=s3_bucket, Key=latest_users_path[locales[0]])
items_locale = pd.read_csv(io.BytesIO(obj['Body'].read()))

# COMMAND ----------

full_accessory_df = pd.DataFrame(columns=['LOCALE', 'ITEM_ID', 'IS_ACCESSORY'])
lista_df = []

for i in range(len(locales)):
    obj = s3_client.get_object(Bucket=s3_bucket, Key=latest_users_path[locales[i]])
    items_locale = pd.read_csv(io.BytesIO(obj['Body'].read()))
    # Create df with local accessory items to append and use later on backup query 
    locale_accessory_df = items_locale[items_locale['IS_ACCESSORY'] == 1][['ITEM_ID', 'IS_ACCESSORY']]
    locale_accessory_df['LOCALE'] = locales[i].upper()  
    full_accessory_df = pd.concat([full_accessory_df,locale_accessory_df], ignore_index=True)
    # Drop duplicates
    items_locale = items_locale.drop_duplicates(subset=['ITEM_ID'])
    # Filter the relevant rows in df_pd_new
    item_ids_to_process = items_locale.ITEM_ID.values
    df_score = pd.DataFrame(columns=['MPVID', 'ITEMLIST', 'LOCALE'])
    # Create a ThreadPoolExecutor with the specified number of threads
    for item_id in item_ids_to_process:
        results = fetch_and_check_recommendations(item_id= item_id, campaign_arn= campaign_arn[locales[i]], filter_arn= accessory_filter_arn[locales[i]])
        # Create a dictionary representing the row you want to add
        new_row = {'MPVID': results[0], 'ITEMLIST':  ', '.join([str(item) for item in results[-1]]), 'LOCALE': locales[i].upper()}
        # Use the append method to add the new row to the DataFrame
        df_score = df_score.append(new_row, ignore_index=True)

    # Apply the custom function to create the 'DEFAULT_FLAG' column
    #df_score['DEFAULT_FLAG'] = df_score['ITEMLIST'].apply(check_default, args=(locales[i],))
    df_score['DEFAULT_FLAG'] = df_score['ITEMLIST'].apply(check_default, args=("".join(list(df_score.ITEMLIST.mode())),))
    # Split the 'list_items_id' strings into separate elements
    df_score['ITEMLIST'] = df_score['ITEMLIST'].str.split(',')

    # Explode the 'list_items_id' column into multiple rows
    df = df_score.explode('ITEMLIST').reset_index(drop=True)

    #Remove any unnoticed white spaces
    df['ITEMLIST'] = df['ITEMLIST'].str.strip()

    # Create initial rank to filter later using ad tec items
    df['PRE_RANK'] = df.groupby('MPVID').cumcount() + 1

    # Reorder columns to match your desired format
    df = df[['MPVID', 'ITEMLIST', 'LOCALE', 'PRE_RANK', 'DEFAULT_FLAG']]
    lista_df.append(df)

# COMMAND ----------

df = pd.concat(lista_df)
df.display()

# COMMAND ----------

# ad tech items that have landing pages (item list subset)
full_ad_tech_df = pd.DataFrame(columns=['item_group_id'])

for i in locales:
    ad_tech_local_items = pd.read_csv(f'fbt_ad_tech_{i}.csv')  
    ad_tech_local_items['LOCALE'] = i.upper()  
    full_ad_tech_df = pd.concat([full_ad_tech_df,ad_tech_local_items], ignore_index=True)

full_ad_tech_df

# COMMAND ----------

full_ad_tech=spark.createDataFrame(full_ad_tech_df) 

# COMMAND ----------

snowflake.table_from_df(
    df=full_ad_tech,
    database=db,
    schema=schema,
    table="fbt_ad_tech_products",
    mode="overwrite")

# COMMAND ----------

# seperate default and non-defualt lists
df_default = df[df['DEFAULT_FLAG'] == 1]
df_default = df_default.rename(columns={'PRE_RANK': 'RANK'})
df_non_default = df[df['DEFAULT_FLAG'] == 0]

# COMMAND ----------

standard_cols = ['MPVID', 'ITEMLIST', 'LOCALE', 'RANK', 'DEFAULT_FLAG']

# COMMAND ----------

# filter non default lists to only include ad tech item subset
df_sims_ad_tech =  pd.merge(df_non_default, full_ad_tech_df, how="inner", left_on=["LOCALE", "ITEMLIST"], right_on=["LOCALE", "item_group_id"])
# create final rank 
df_sims_ad_tech['RANK'] = df_sims_ad_tech.\
                                sort_values(['PRE_RANK'], ascending=True).\
                                groupby(['LOCALE', 'MPVID']).cumcount() + 1
df_sims_ad_tech = df_sims_ad_tech[df_sims_ad_tech['RANK'] <= 20].sort_values(['LOCALE','MPVID','RANK'], ascending=True)

# COMMAND ----------

# flag any items that do not have a suficcient enough product list (< 3) 
# considered default and will be filtered before delivering
df_sims_ad_tech['MAX_RANK'] = df_sims_ad_tech.groupby(['MPVID','LOCALE'])['RANK'].transform('max')
df_sims_ad_tech['DEFAULT_FLAG'] = np.where(df_sims_ad_tech['MAX_RANK'] < item_list_min, 1, 0)

# COMMAND ----------

df_sufficient = df_sims_ad_tech[df_sims_ad_tech['DEFAULT_FLAG'] == 0][standard_cols]
df_sufficient

# COMMAND ----------

df_insufficient = df_sims_ad_tech[df_sims_ad_tech['DEFAULT_FLAG'] == 1][standard_cols]
df_insufficient

# COMMAND ----------

query = f'''with mpv_ids as (select country_code, mpv_id, product_key, valid_from, valid_to
from vistaprint.product.product_mpv 
where merchant = 'VISTAPRINT'
qualify row_number() over(partition by country_code, product_key order by etl_timestamp desc, created_at desc) = 1),

order_product_set as (
    select distinct
        crm.recs_model_region,
        lbr.order_number,
        mpv.mpv_id,
        count(distinct mpv.mpv_id) over (partition by crm.recs_model_region, lbr.order_number) as order_size
    from vistaprint.transactions.lob_business_review lbr
        join dna.personalization.recs_country_region_mapping crm on lbr.country = crm.country
        join mpv_ids mpv
            on lbr.product_key = mpv.product_key
            and lbr.order_created_datetime between mpv.valid_from and mpv.valid_to
            and lbr.country = case when mpv.country_code = 'GB' then 'UK' else mpv.country_code end
    where lbr.order_created_date >= dateadd(month, -15, sysdate()) -- Last 15 months
),
co_purchases as (
    select
        s1.recs_model_region,
        s1.mpv_id as source_mpv_id,
        s2.mpv_id as combo_mpv_id,
        count(distinct s1.order_number) as order_count, -- Number of co-purchases
        avg(s1.order_size) as avg_order_size
    from order_product_set s1
        join order_product_set s2
            on s1.order_number = s2.order_number
            and s1.mpv_id <> s2.mpv_id
    group by
        s1.recs_model_region, s1.mpv_id, s2.mpv_id
    having order_count >= 2
),

ad_tech_filtered as (
    select cp.*
    from co_purchases cp
        join {db}.{schema}.fbt_ad_tech_products at
        on upper(cp.recs_model_region) = upper(at.locale)
        and cp.combo_mpv_id = at.item_group_id
),
total_orders as (
    -- Calculate total orders for source product + support(y)
    select
        recs_model_region,
        mpv_id,
        count(distinct order_number) as distinct_orders,
        ratio_to_report(distinct_orders) over(partition by recs_model_region) as supp_y
    from order_product_set
    group by recs_model_region, mpv_id
),
scores as (
    select
        cp.source_mpv_id,
        cp.combo_mpv_id,
        cp.recs_model_region,
        cp.order_count,
        cp.avg_order_size,
        cp.order_count * 1.0 / t_source.distinct_orders as confidence, -- Confidence calculation
        t_combo.supp_y,
        (1 - t_combo.supp_y) / (1 - confidence + 0.0000000001) as conviction,
    from ad_tech_filtered cp
        join total_orders t_source
            on cp.recs_model_region = t_source.recs_model_region
            and cp.source_mpv_id = t_source.mpv_id
        join total_orders t_combo
            on cp.recs_model_region = t_combo.recs_model_region
            and cp.combo_mpv_id = t_combo.mpv_id
),

rankings as (
    select 
    source_mpv_id as mpvid, 
    combo_mpv_id as itemlist, 
    upper(recs_model_region) as locale, 
    order_count, 
    avg_order_size,
    confidence,
    supp_y,
    conviction,
    count(source_mpv_id) over(partition by recs_model_region, source_mpv_id) as itemlist_count 
    from scores  
    qualify itemlist_count >= {item_list_min}
)

select *
from rankings '''

df2 = snowflake.execute_reader(query)
df_manual_sims = df2.toPandas()

# COMMAND ----------

#Remove accessory products from manual item list and rank 
df_manual_sims_filt =  pd.merge(df_manual_sims, full_accessory_df, how="left", left_on=["LOCALE", "ITEMLIST"], right_on=["LOCALE", "ITEM_ID"])
df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['IS_ACCESSORY'].isna()]
df_manual_sims_filt['RANK'] = df_manual_sims_filt.\
                                sort_values(['CONVICTION', 'CONFIDENCE', 'SUPP_Y', 'AVG_ORDER_SIZE'], ascending=[False,False,False,True]).\
                                groupby(['LOCALE', 'MPVID']).cumcount() + 1

# flag any items that do not have a suficcient enough product list (< 3) 
# considered default and will be filtered before delivering
df_manual_sims_filt['MAX_RANK'] = df_manual_sims_filt.groupby(['MPVID','LOCALE'])['RANK'].transform('max')
df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['MAX_RANK'] >= item_list_min]
df_manual_sims_filt = df_manual_sims_filt[df_manual_sims_filt['RANK'] <= 20].sort_values(['LOCALE','MPVID','RANK'], ascending=True)

# COMMAND ----------

df_manual_sims_filt['IS_ACCESSORY'].sum()

# COMMAND ----------

# combine all seperated dfs together for backup query processing
df = pd.concat([df_default, df_insufficient, df_sufficient])

# COMMAND ----------

df

# COMMAND ----------

df_manual_sims_filt

# COMMAND ----------

locales = df.LOCALE.unique()
df_manual_sims_filt = df_manual_sims_filt.drop(columns={'ITEMLIST_COUNT','ORDER_COUNT','AVG_ORDER_SIZE','ITEM_ID','IS_ACCESSORY','MAX_RANK','CONFIDENCE','SUPP_Y','CONVICTION'})
df_manual_sims_filt['DEFAULT_FLAG'] = 0
#replace default recommendations with manual query iterating by locale
for i in locales:
    #extract all default recommendations for locale i
    df_filtered = df[(df['DEFAULT_FLAG'] == 1)&(df['LOCALE'] == i)]
    #get all recommendations from manual query for locale i
    df_sims = df_manual_sims_filt[df_manual_sims_filt['LOCALE'] == i]
    #get common MPVIDs for default recommendations and manual query
    common_mpvids = pd.Series(list(set(df_filtered['MPVID']).intersection(set(df_sims['MPVID']))))
    #drop default recommendations for locale i that have a common MPVID in manual query
    df = df[~((df['LOCALE'] == i) & (df['MPVID'].isin(common_mpvids)))]
    #add manual query recommendations for locale i to the original dataframe
    df = pd.concat([df, df_sims[df_sims['MPVID'].isin(common_mpvids)]], ignore_index=True)

df_all = df.drop_duplicates().reset_index(drop=True)

# COMMAND ----------

df_all

# COMMAND ----------

# Convert the pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(df_all)

# Show the Spark DataFrame
spark_df.display()

# COMMAND ----------

snowflake.table_from_df(spark_df, db, schema, 'SIMS_ITEMS_AD_TECH', mode='overwrite')
