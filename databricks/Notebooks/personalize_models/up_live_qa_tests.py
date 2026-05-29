# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

import requests
import importlib
from pyspark.sql import Row
from pyspark.sql.functions import col, row_number, current_timestamp, lit, expr, round, split, when, array_contains
from pyspark.sql.window import Window
import pprint

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

print(f"""
environment: {environment}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
aws_session = utils.get_temp_session(temp_creds)
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

anonymous_token = requests.request(
    method="POST",
    url=dbutils.secrets.get(scope="personalizationProductRecommender", key="tempTokenUrl"),
    headers={},
    data={}
).json()["anonymousToken"]

def get_recommended_items(model_id, poet_model_version, locale, count=10, mpv_id="", page_section="", user_id="non-existing-user"):
    endpoint_env = "" if environment == "prd" else "-dev"
    item_param = f"&itemId={mpv_id}" if ("sims" in model_id or "category" in model_id) else ""
    score_path = "category/score" if "category" in model_id else "score"
    url = (f"https://recommendations{endpoint_env}.dna.vpsvc.com/v1/{score_path}"
           f"?modelVersion={poet_model_version}"
           f"&count={count}"
           f"&context=%7B%22locale%22%3A%22{locale}%22%2C%22page_section%22%3A%22{page_section}%22%7D"
           f"&userId={user_id}"
           f"&modelId={model_id}"
           f"&locale={utils.region_code[locale]}"
           f"{item_param}")

    print(f"Testing URL: {url}")

    return requests.request(
        method="GET",
        url=url,
        headers={"Authorization": f"Bearer {anonymous_token}"},
        data={}
    ).json()

# COMMAND ----------

def get_up_solution_versions(dataset_group_arn_list):
    solution_version_arns = []
    for dg_arn in dataset_group_arn_list:
        solutions_response = personalize.list_solutions(datasetGroupArn=dg_arn)
        up_solutions = [sol for sol in solutions_response['solutions'] if 'up_sol' in sol['solutionArn'].lower()]
        if not up_solutions:
            continue
        solution_arn = up_solutions[0]['solutionArn']
        solutions_version_response = personalize.list_solution_versions(solutionArn=solution_arn)
        try:
            next_token = solutions_version_response.get('nextToken')
            while next_token:
                additional_solution_version_data = personalize.list_solution_versions(solutionArn=solution_arn, nextToken=next_token)
                solutions_version_response['solutionVersions'] += additional_solution_version_data['solutionVersions']
                next_token = additional_solution_version_data.get('nextToken')
        except:
            pass
        for sv in solutions_version_response['solutionVersions']:
            sv['datasetGroupArn'] = dg_arn
            solution_version_arns.append(sv)
    return solution_version_arns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cold Start Users

# COMMAND ----------

dataset_groups_response = personalize.list_dataset_groups()

# COMMAND ----------

dataset_groups_response

# COMMAND ----------

dataset_group_names = []
dataset_group_arns = []
prd_poet_tags = {}
region_data = {}

for dg in dataset_groups_response['datasetGroups']:
    try:
        arn = dg['datasetGroupArn']
        name = dg['name']
        tag = personalize.list_tags_for_resource(resourceArn=arn)["tags"][0]["tagValue"]
        if 'test' not in tag.lower() and 'sept_2025' not in tag.lower():
            dataset_group_names.append(name)
            dataset_group_arns.append(arn)
            prd_poet_tags[name] = tag
            region, model_version, version = name.split("_")
            region_data[region] = {'version': version, 'poet_tag': tag}
    except:
        pass

# COMMAND ----------

pprint.pprint(f"""prd_poet_tag: {prd_poet_tags}""")

# COMMAND ----------

solution_version_arns = get_up_solution_versions(dataset_group_arns)

# COMMAND ----------

solution_version_arn_cols = ['datasetGroupArn','solutionVersionArn','creationDateTime','status']

rows = []
for i in solution_version_arns:
    row_dict = {col: i.get(col, None) for col in solution_version_arn_cols}
    rows.append(Row(**row_dict))

solutions_verion_arns_df = spark.createDataFrame(rows
                                                 , schema='datasetGroupArn string, solutionVersionArn string, creationDateTime timestamp, status string')

# COMMAND ----------

solutions_verion_arns_df = solutions_verion_arns_df.withColumn(
    'solutionVersion',
    expr("substring(solutionVersionArn, 1, length(solutionVersionArn) - 9)")
)

solutions_verion_arns_df = solutions_verion_arns_df.where(
    col('status') == 'ACTIVE'
)

window_spec = Window.partitionBy("solutionVersion").orderBy(
    col("creationDateTime").desc()
)

current_solution_version_arn_df = solutions_verion_arns_df.withColumn(
    "solution_recency_rank",
    row_number().over(window_spec)
).filter(
    col("solution_recency_rank") == 1
)

current_solution_version_arn_df = current_solution_version_arn_df.withColumns({
    "DAYS_SINCE_RETRAIN": round(
        (current_timestamp() - col("creationDateTime")).cast("int") / 86400
    ),
    "NAME": split(col("datasetGroupArn"), '/').getItem(1)
})

# COMMAND ----------

current_solution_version_arn_df.display()

# COMMAND ----------

PRODUCT_CLICKED_COUNTS_SQL = '''
with product_viewed_events
        as
        (
            SELECT NVL(pv.USER_ID, ui2.VALUE) AS USER_ID,
                   pv.PRODUCT_ID AS ITEM_ID,
                   DATE_PART(EPOCH_SECOND, pv.TIMESTAMP) AS TIMESTAMP,
                   pv.PAGE_SECTION,
                   pv.LOCALE,
                   rank() over(partition by pv.CONTEXT_PAGE_SEARCH order by pv.TIMESTAMP) as r,
                   row_number() over (partition by pv.visit, pv.product_id, pv.page_section order by pv.timestamp ) as row_num 
            FROM VISTAPRINT.WEB_TRACKING_EVENTS.product_viewed pv
                LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui
                    ON pv.USER_ID IS NULL 
                    AND ui.TYPE = 'anonymous_id' 
                    AND ui.VALUE = pv.anonymous_id
                LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui2
                    ON ui2.TYPE = 'user_id' 
                    AND ui.CANONICAL_SEGMENT_ID = ui2.CANONICAL_SEGMENT_ID
                LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS u 
                    ON u.ID = pv.USER_ID
            WHERE (
                  (  pv.USER_ID IS NOT NULL AND pv.USER_ID not LIKE '%@%') 
                  OR
                  ( ui2.VALUE IS NOT NULL AND ui2.VALUE not LIKE '%@%')
                  )
            AND pv.PRODUCT_ID NOT LIKE '%PRD-%'
            AND (pv.PAGE_SECTION in ('Configure - Recommendation', 'Gallery')
            --exclude Studio's product views that already have a product clicked event in recommendation matching components
            OR (pv.PAGE_SECTION = 'Studio' AND pv.CONTEXT_PAGE_URL NOT LIKE '%recommendationsId=%')
            --exclude Product Page viewed events that come from Cross Sell static recommendation components
            OR (pv.PAGE_SECTION = 'Product Page' AND pv.CONTEXT_PAGE_REFERRER NOT LIKE '%/xs/%'))
            AND pv.TIMESTAMP > dateadd(month, -15, current_date()) AND pv.TIMESTAMP < current_date()
            AND ifnull(u.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) =1
        ),

ranked_views as (
        SELECT DISTINCT USER_ID, 
               ITEM_ID, 
               TIMESTAMP, 
               PAGE_SECTION, 
               LOCALE,
               CASE 
               WHEN PAGE_SECTION = 'Configure - Recommendation' THEN 'addToCart'
               WHEN PAGE_SECTION IN ('Gallery', 'Studio') THEN 'click'
               ELSE 'productView' END AS EVENT_TYPE
        FROM product_viewed_events
        WHERE ( PAGE_SECTION <> 'Configure - Recommendation' or r=1 ) 
        and not (page_section in ('Gallery', 'Studio') and row_num > 1)),

product_clicked as (SELECT DISTINCT NVL(pv.USER_ID, ui2.VALUE) AS USER_ID,
               pv.PRODUCT_ID AS ITEM_ID,
               DATE_PART(EPOCH_SECOND, pv.TIMESTAMP) AS TIMESTAMP,
               pv.PAGE_SECTION,
               pv.LOCALE,
               'click' as EVENT_TYPE
        FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_CLICKED pv
            LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui
                ON pv.USER_ID IS NULL 
                AND ui.TYPE = 'anonymous_id' 
                AND ui.VALUE=pv.anonymous_id
            LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui2
                ON ui2.TYPE = 'user_id' 
                AND ui.CANONICAL_SEGMENT_ID=ui2.CANONICAL_SEGMENT_ID
            LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS u 
                ON u.ID = pv.USER_ID
        WHERE (
              ( pv.USER_ID IS NOT NULL AND pv.USER_ID NOT LIKE '%@%') 
              OR
              ( ui2.VALUE IS NOT NULL AND ui2.VALUE NOT LIKE '%@%')
              )
        AND pv.PRODUCT_ID NOT LIKE '%PRD-%'
        AND pv.TIMESTAMP > dateadd(month, -15, current_date()) AND pv.TIMESTAMP < current_date()
        AND ifnull(u.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
        AND pv.route = 'recommendations'
        AND (
        pv.PAGE_SECTION IN ('Design Services', 'Configure - Recommendation', 'My Account')
        OR (
            pv.PAGE_SECTION = 'Cart'
            --include only product clicked events from matching
            AND NVL(LOWER(pv.LIST_SECTION_ID), LOWER(pv.OFFER_TYPE)) LIKE '%matching%'
        )
        OR (
            pv.PAGE_SECTION = 'Home Page'
            --include only product clicked events from matching
            AND NVL(LOWER(pv.LIST_SECTION_ID), LOWER(pv.OFFER_TYPE)) LIKE '%matching%'
        ))
),

engagement as (
select * from ranked_views

union all

select * from product_clicked)


select locale, item_id, count(*) as clicks, row_number() over(partition by locale order by clicks desc) as click_rank
from engagement
group by locale, item_id
'''

# COMMAND ----------

print(PRODUCT_CLICKED_COUNTS_SQL)
clicked_counts = snowflake.execute_reader(PRODUCT_CLICKED_COUNTS_SQL).cache()

# COMMAND ----------

cold_start_data = []

for dg_name in dataset_group_names:
    region, model_version, version = dg_name.split("_")

    for locale in utils.region_locale[region]:
        response = get_recommended_items(f"{version}_user_personalization", prd_poet_tags[dg_name], locale)
        for rank, item in enumerate(sorted(response["itemList"], key=lambda x: x["score"], reverse=True), start=1):
            cold_start_data.append((dg_name, region, locale, item["itemId"], item["score"], rank))

cold_start_df = spark.createDataFrame(cold_start_data, ["DATASET_GROUP", "REGION", "LOCALE", "ITEM_ID", "SCORE", "LOCALE_RANK"])

# COMMAND ----------

cold_start_df.display()

# COMMAND ----------

prd_poet_tags_df = spark.createDataFrame(
    [(k, v) for k, v in prd_poet_tags.items()],
    ["DATASET_GROUP", "POET_MODEL_VERSION"]
)

# COMMAND ----------

combined_df_cs = (
    cold_start_df
    .join(clicked_counts, on=["LOCALE", "ITEM_ID"], how="left")
    .join(
        current_solution_version_arn_df,
        cold_start_df.DATASET_GROUP == current_solution_version_arn_df.NAME,
        how="left"
    )
    .join(
        prd_poet_tags_df,
        cold_start_df.DATASET_GROUP == prd_poet_tags_df.DATASET_GROUP,
        how="left"
    )
    .withColumn("CREATED_AT", current_timestamp())
    .withColumn("MODEL_VERSION", lit(version))
    .select(
        "LOCALE",
        "ITEM_ID",
        "REGION",
        col("SCORE").alias("MODEL_SCORE"),
        col("LOCALE_RANK").alias("LOCALE_MODEL_RANK"),
        "CLICKS",
        col("CLICK_RANK").alias("LOCALE_CLICK_RANK"),
        "DAYS_SINCE_RETRAIN",
        "MODEL_VERSION",
        "POET_MODEL_VERSION",
        "CREATED_AT"
    )
)

combined_df_cs.display()

# COMMAND ----------

print(f"{utils.sf_db}.{utils.sf_schema}")

# COMMAND ----------

snowflake.table_from_df(
    df=combined_df_cs,
    database=utils.sf_db,
    schema=utils.sf_schema,
    table=f"up_cold_start_qa",
    mode="append")

# COMMAND ----------

snowflake.execute_nonquery(f"""                    
DELETE FROM {utils.sf_db}.{utils.sf_schema}.up_cold_start_qa
WHERE CREATED_AT < add_months(current_date(), -13)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Existing Users

# COMMAND ----------

user_sample_engagement_query = f"""
with product_viewed_events
        as
        (
            SELECT NVL(pv.USER_ID, ui2.VALUE) AS USER_ID,
                   pv.PRODUCT_ID AS ITEM_ID,
                   DATE_PART(EPOCH_SECOND, pv.TIMESTAMP) AS TIMESTAMP,
                   pv.PAGE_SECTION,
                   pv.LOCALE,
                   rank() over(partition by pv.CONTEXT_PAGE_SEARCH order by pv.TIMESTAMP) as r,
                   row_number() over (partition by pv.visit, pv.product_id, pv.page_section order by pv.timestamp ) as row_num 
            FROM VISTAPRINT.WEB_TRACKING_EVENTS.product_viewed pv
                LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui
                    ON pv.USER_ID IS NULL 
                    AND ui.TYPE = 'anonymous_id' 
                    AND ui.VALUE = pv.anonymous_id
                LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui2
                    ON ui2.TYPE = 'user_id' 
                    AND ui.CANONICAL_SEGMENT_ID = ui2.CANONICAL_SEGMENT_ID
                LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS u 
                    ON u.ID = pv.USER_ID
            WHERE (
                  (  pv.USER_ID IS NOT NULL AND pv.USER_ID not LIKE '%@%') 
                  OR
                  ( ui2.VALUE IS NOT NULL AND ui2.VALUE not LIKE '%@%')
                  )
            AND pv.PRODUCT_ID NOT LIKE '%PRD-%'
            AND (pv.PAGE_SECTION in ('Configure - Recommendation', 'Gallery')
            --exclude Studio's product views that already have a product clicked event in recommendation matching components
            OR (pv.PAGE_SECTION = 'Studio' AND pv.CONTEXT_PAGE_URL NOT LIKE '%recommendationsId=%')
            --exclude Product Page viewed events that come from Cross Sell static recommendation components
            OR (pv.PAGE_SECTION = 'Product Page' AND pv.CONTEXT_PAGE_REFERRER NOT LIKE '%/xs/%'))
            AND pv.TIMESTAMP > dateadd(month, -6, current_date()) AND pv.TIMESTAMP < current_date()
            AND ifnull(u.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) =1
        ),

ranked_views as (
        SELECT DISTINCT USER_ID, 
               ITEM_ID, 
               TIMESTAMP, 
               PAGE_SECTION, 
               LOCALE,
               CASE 
               WHEN PAGE_SECTION = 'Configure - Recommendation' THEN 'addToCart'
               WHEN PAGE_SECTION IN ('Gallery', 'Studio') THEN 'click'
               ELSE 'productView' END AS EVENT_TYPE
        FROM product_viewed_events
        WHERE ( PAGE_SECTION <> 'Configure - Recommendation' or r=1 ) 
        and not (page_section in ('Gallery', 'Studio') and row_num > 1)),

product_clicked as (SELECT DISTINCT NVL(pv.USER_ID, ui2.VALUE) AS USER_ID,
               pv.PRODUCT_ID AS ITEM_ID,
               DATE_PART(EPOCH_SECOND, pv.TIMESTAMP) AS TIMESTAMP,
               pv.PAGE_SECTION,
               pv.LOCALE,
               'click' as EVENT_TYPE
        FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_CLICKED pv
            LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui
                ON pv.USER_ID IS NULL 
                AND ui.TYPE = 'anonymous_id' 
                AND ui.VALUE=pv.anonymous_id
            LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS ui2
                ON ui2.TYPE = 'user_id' 
                AND ui.CANONICAL_SEGMENT_ID=ui2.CANONICAL_SEGMENT_ID
            LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS u 
                ON u.ID = pv.USER_ID
        WHERE (
              ( pv.USER_ID IS NOT NULL AND pv.USER_ID NOT LIKE '%@%') 
              OR
              ( ui2.VALUE IS NOT NULL AND ui2.VALUE NOT LIKE '%@%')
              )
        AND pv.PRODUCT_ID NOT LIKE '%PRD-%'
        AND pv.TIMESTAMP > dateadd(month, -6, current_date()) AND pv.TIMESTAMP < current_date()
        AND ifnull(u.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
        AND pv.route = 'recommendations'
        AND (
        pv.PAGE_SECTION IN ('Design Services', 'Configure - Recommendation', 'My Account')
        OR (
            pv.PAGE_SECTION = 'Cart'
            --include only product clicked events from matching
            AND NVL(LOWER(pv.LIST_SECTION_ID), LOWER(pv.OFFER_TYPE)) LIKE '%matching%'
        )
        OR (
            pv.PAGE_SECTION = 'Home Page'
            --include only product clicked events from matching
            AND NVL(LOWER(pv.LIST_SECTION_ID), LOWER(pv.OFFER_TYPE)) LIKE '%matching%'
        ))
),

engagement as (
select * from ranked_views

union all

select * from product_clicked),

user_sample_base as (
select distinct user_id, count(*) over(partition by user_id) as eng_count, first_value(LOCALE) over(partition by user_id order by TIMESTAMP desc) as user_locale
, case when eng_count = 1 then '1. 1' 
       when eng_count between 2 and 10 then '2. 2-10'
       when eng_count between 11 and 49 then '3. 11-49'
       else '4. 50+' end as eng_count_grouped
from engagement
),

user_sample as (
select u.*, recs_model_region
from user_sample_base u
join dna.personalization.recs_country_region_mapping crm
on u.user_locale = crm.country
where eng_count_grouped = '1. 1'
QUALIFY ROW_NUMBER() OVER (partition by recs_model_region ORDER BY RANDOM()) = 1

union all

select u.*, crm.recs_model_region
from user_sample_base u
join dna.personalization.recs_country_region_mapping crm
on u.user_locale = crm.country
where eng_count_grouped = '2. 2-10'
QUALIFY ROW_NUMBER() OVER (partition by recs_model_region ORDER BY RANDOM()) = 1

union all

select u.*, crm.recs_model_region
from user_sample_base u
join dna.personalization.recs_country_region_mapping crm
on u.user_locale = crm.country
where eng_count_grouped = '3. 11-49'
QUALIFY ROW_NUMBER() OVER (partition by recs_model_region ORDER BY RANDOM()) = 1

union all

select u.*, crm.recs_model_region
from user_sample_base u
join dna.personalization.recs_country_region_mapping crm
on u.user_locale = crm.country
where eng_count_grouped = '4. 50+'
QUALIFY ROW_NUMBER() OVER (partition by recs_model_region ORDER BY RANDOM()) = 1

),

engagement_counts as (
select u.RECS_MODEL_REGION, eng_count_grouped, u.user_locale, u.USER_ID, b.subcategory, COUNT(*) AS NUM_INTERACTIONS
from user_sample u
join engagement e on u.user_id = e.user_id
join dna.personalization.recommendations_base_items b
on e.item_id = b.mpv_id
group by 1,2,3,4,5)


select RECS_MODEL_REGION, eng_count_grouped, user_locale, USER_ID, LISTAGG(subcategory, ',') within group (order by NUM_INTERACTIONS desc) as engaged_subcategories
from engagement_counts
group by 1,2,3,4
order by 1,2,3
"""

# COMMAND ----------

sample_user_engagement = snowflake.execute_reader(user_sample_engagement_query).cache()
sample_user_engagement = sample_user_engagement.withColumn(
    "ENGAGED_SUBCATEGORIES",
    split("ENGAGED_SUBCATEGORIES", ",")
)

# COMMAND ----------

sample_user_engagement.display()

# COMMAND ----------

base_items_query = """
select *
from dna.personalization.recommendations_base_items
"""

base_items = snowflake.execute_reader(base_items_query).cache()

# COMMAND ----------

distinct_users = sample_user_engagement.select("user_id", "USER_LOCALE", "RECS_MODEL_REGION").distinct()
user_locale_pairs = distinct_users.collect()

# COMMAND ----------

region_data

# COMMAND ----------

recommendation_results = []

for row in user_locale_pairs:
    user_id = row["user_id"]
    locale = row["USER_LOCALE"]
    region = row["RECS_MODEL_REGION"]
    version = region_data[region]['version']
    poet_tag = region_data[region]['poet_tag']

    response = get_recommended_items(f"{version}_user_personalization", poet_tag, user_id=user_id, locale=locale, count=5)
    for rank, item in enumerate(sorted(response["itemList"], key=lambda x: x["score"], reverse=True), start=1):
        recommendation_results.append((user_id, locale, item["itemId"], item["score"], rank))

recommendation_df = spark.createDataFrame(
    recommendation_results, ["USER_ID", "USER_LOCALE", "ITEM_ID", "SCORE", "RECOMMENDATION_RANK"]
)

recommendation_df = recommendation_df.join(base_items.select("MPV_ID", "SUBCATEGORY", "COUNTRY"), on= ((recommendation_df.ITEM_ID == base_items.MPV_ID) & (recommendation_df.USER_LOCALE == base_items.COUNTRY)) , how="left").drop('COUNTRY', "MPV_ID")

# COMMAND ----------

combined_df_eu = recommendation_df.join(
    sample_user_engagement,
    on = ["USER_ID", "USER_LOCALE"],
    how="left"
)

combined_df_eu = combined_df_eu.withColumn('ENGAGED_SUBCATEGORY_FLAG', 
                                     when(array_contains(
                        sample_user_engagement.ENGAGED_SUBCATEGORIES, recommendation_df.SUBCATEGORY)
                                          , 1).otherwise(0)) \
                         .withColumn('CREATED_AT', current_timestamp())

combined_df_eu = combined_df_eu.withColumnRenamed('ITEM_ID', 'RECOMMENDED_ITEM_ID') \
               .withColumnRenamed('SCORE', 'MODEL_SCORE') \
               .withColumnRenamed('SUBCATEGORY', 'RECOMMENDED_ITEM_SUBCATEGORY')

# COMMAND ----------

combined_df_eu.display()

# COMMAND ----------

snowflake.table_from_df(
    df=combined_df_eu,
    database=utils.sf_db,
    schema=utils.sf_schema,
    table=f"up_existing_user_qa",
    mode="append")

# COMMAND ----------

snowflake.execute_nonquery(f"""                    
DELETE FROM {utils.sf_db}.{utils.sf_schema}.up_existing_user_qa
WHERE CREATED_AT < add_months(current_date(), -13)
"""
)

# COMMAND ----------


