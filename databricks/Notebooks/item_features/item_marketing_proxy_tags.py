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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, MapType, DoubleType,StringType, FloatType
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, round, when, lit, pow, current_timestamp, log
from pyspark.sql.window import Window

# COMMAND ----------

@udf(FloatType()) 
def bayes_avg(sys_weight, sys_rating_avg, item_rating_cnt, item_rating_avg): 
    bayes_rating = ((sys_weight * sys_rating_avg) + (item_rating_cnt * item_rating_avg)) / (sys_weight + item_rating_cnt)
    return(bayes_rating)

# COMMAND ----------

# MAGIC %md
# MAGIC ### High Value

# COMMAND ----------

high_value_proxy_query = f"""
create or replace table {db}.{schema}.product_high_value_proxy as
with product_universe as (
    select distinct b.recs_model_region, b.product_key, b.mpv_id, b.category, b.subcategory, b.product_group, b.moq, 
    b.total_moq_price * br.rate as moq_total_price_usd,
    b.unit_moq_price * br.rate as moq_unit_price_usd
    from dna.personalization.recommendations_base_items b
        left JOIN VISTAPRINT.TRANSACTIONS.DIM_BUDGET_RATES br ON b.currency = br.currency_from
        AND br.DATE = DATE (DATEADD(day, - 1, CURRENT_DATE))
        AND br.currency_to = 'USD'
    where b.category not in ('Samples')
),

region_prod_avg_prices as (
    select -- final unqiue grain = product x region
        -- avg price across all config options per product x region
        recs_model_region,
        category,
        subcategory,
        product_group,
        mpv_id,
        min(moq) as default_moq,
        avg(moq_unit_price_usd) as prod_region_avg_unit_price
    from product_universe
    group by all
),

cleaned_prices as (
select distinct p.*,
    COUNT(DISTINCT mpv_id) OVER(PARTITION by recs_model_region, category) as region_category_count,
    median(prod_region_avg_unit_price) OVER(PARTITION by recs_model_region, subcategory) as median_subcategory_price,
    median(prod_region_avg_unit_price) OVER(PARTITION by recs_model_region, product_group) as median_product_group_price,
    coalesce(prod_region_avg_unit_price, median_product_group_price, median_subcategory_price) as calc_moq_unit_price_usd,
    case when prod_region_avg_unit_price is null then 1 else 0 end as median_price_flag,
    AVG(prod_region_avg_unit_price) OVER(PARTITION by recs_model_region, category) as avg_category_price,
    calc_moq_unit_price_usd / avg_category_price as product_ap_ratio
from region_prod_avg_prices p)

select c.*,
    percent_rank() over(
        partition by recs_model_region,
        category
        order by calc_moq_unit_price_usd asc
    ) as pct_rank,
case
        when pct_rank <= 0.25 then '4_least'
        when pct_rank > 0.25 and pct_rank <= 0.50 then '3_low'
        when pct_rank > 0.50 and pct_rank <= 0.75 then '2_medium'
        when pct_rank > 0.75 then '1_high'
    end as high_value_proxy_category,
    sysdate() as created_at
from cleaned_prices c
"""

print(high_value_proxy_query)

# COMMAND ----------

snowflake.execute_nonquery(high_value_proxy_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bulk

# COMMAND ----------

bulk_proxy_query = f"""with product_median_quantity as (select
    b.recs_model_region,
    b.category,
    b.subcategory,
    b.product_group,
    b.mpv_id,
    count(distinct order_number) as order_count,
    median(quantity) as product_median_quantity
from
    vistaprint.transactions.lob_business_review lbr
join vistaprint.product.product_mpv m
    on lbr.product_key = m.product_key
    and lbr.country = case when m.country_code = 'GB' then 'UK' else m.country_code end
    and CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime) >= m.valid_from
    and CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime) < m.valid_to
    and m.merchant = 'VISTAPRINT'
join dna.personalization.recommendations_base_items b 
    on m.mpv_id = b.mpv_id
    and m.country_code = b.country
where lbr.product_key is not null
and lbr.order_created_date >= dateadd ('year', '-2', date (sysdate()))
and b.category not in ('Design','Samples', 'Digital','Other')
group by
    1,2,3,4,5),
   
-- SYSTEM_WEIGHT
system_prior_weight as (
select distinct pm.recs_model_region
    ,pm.category
    ,pm.subcategory
    ,pm.product_group
    ,APPROX_PERCENTILE(order_count, 0.20) over(partition by recs_model_region, category, subcategory, product_group) as system_weight
from product_median_quantity pm),

system_attributes as (select
    b.recs_model_region,
    b.category,
    b.subcategory,
    b.product_group,
    count(distinct order_number) as system_order_count,
    median(quantity) as system_quant_med
from
    vistaprint.transactions.lob_business_review lbr
join vistaprint.product.product_mpv m 
    on lbr.product_key = m.product_key
    and lbr.country = case when m.country_code = 'GB' then 'UK' else m.country_code end
    and CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime) >= m.valid_from
    and CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime) < m.valid_to
    and m.merchant = 'VISTAPRINT'
join dna.personalization.recommendations_base_items b
    on m.mpv_id = b.mpv_id
    and m.country_code = b.country
    where lbr.product_key is not null
    and lbr.order_created_date >= dateadd ('year', '-2', date (sysdate()))
group by
    1,2,3,4),
    
system_final as (select
    sw.recs_model_region
    ,sw.category
    ,sw.subcategory
    ,sw.product_group
    ,sw.system_weight
    ,sa.system_order_count
    ,sa.system_quant_med
from
    system_prior_weight sw
    join system_attributes sa on sw.recs_model_region = sa.recs_model_region
    and sw.category = sa.category 
    and sw.subcategory = sa.subcategory
    and sw.product_group = sa.product_group)
 

select pmq.*,
s.system_weight,
s.system_order_count,
s.system_quant_med
from product_median_quantity pmq
join system_final s
on s.recs_model_region = pmq.recs_model_region
and s.category = pmq.category
and s.subcategory = pmq.subcategory
and s.product_group = pmq.product_group
"""

print(bulk_proxy_query)

# COMMAND ----------

cols_to_convert = ['SYSTEM_WEIGHT','SYSTEM_QUANT_MED','ORDER_COUNT','PRODUCT_MEDIAN_QUANTITY']

bulk_df = snowflake.execute_reader(bulk_proxy_query).cache()
for col_name in cols_to_convert:
    bulk_df = bulk_df.withColumn(col_name, col(col_name).cast('float'))

# COMMAND ----------

bulk_df = bulk_df.withColumn("CALC_MEDIAN_QUANTITY", \
                        when(bulk_df.ORDER_COUNT < 50, \
                            round(bayes_avg("SYSTEM_WEIGHT", "SYSTEM_QUANT_MED", "ORDER_COUNT", "PRODUCT_MEDIAN_QUANTITY"))) \
                            .otherwise(bulk_df.PRODUCT_MEDIAN_QUANTITY))

# COMMAND ----------

window_spec = Window.partitionBy("RECS_MODEL_REGION").orderBy("CALC_MEDIAN_QUANTITY")
bulk_df = bulk_df.withColumn("percentile", percent_rank().over(window_spec))

bulk_df = bulk_df.withColumn(
    "BULK_PROXY_CATEGORY",
    when(col("percentile") <= 0.25, '4_least')
    .when((col("percentile") > 0.25) & (col("percentile") <= 0.5), '3_low')
    .when((col("percentile") > 0.5) & (col("percentile") <= 0.75), '2_medium')
    .otherwise('1_high'))

bulk_df = bulk_df.drop("percentile")
bulk_df = bulk_df.withColumn("CREATED_AT", current_timestamp())

# COMMAND ----------

snowflake.table_from_df(
    df=bulk_df,
    database=db,
    schema=schema,
    table=f"product_bulk_proxy",
    mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ratings

# COMMAND ----------

ratings_proxy_query = f"""
with product_attributes as (
select
    b.recs_model_region
    ,b.category
    ,b.subcategory
    ,b.product_group
    ,b.mpv_id
    ,count(distinct review_id) as product_rating_count
    ,avg(rating) as product_rating_avg    
from
    vistaprint.product.product_review pr
    join vistaprint.product.product_categorization pc
    on pr.product_key = pc.product_key
    left join vistaprint.transactions.lob_business_review lbr
        on pr.order_number = lbr.order_number
        and pr.order_item_id = lbr.item_id
    join vistaprint.product.product_mpv m 
        on pr.product_key = m.product_key
        and pr.country_code = m.country_code
        and coalesce(CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime), pr.created_at) >= m.valid_from
        and coalesce(CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime), pr.created_at) < m.valid_to
        and m.merchant = 'VISTAPRINT'
    join dna.personalization.recommendations_base_items b
        on m.mpv_id = b.mpv_id
        and m.country_code = b.country
where published_flag = 1
and pc.category not in ('Design','Samples', 'Digital','Other')
group by 1,2,3,4,5),

system_prior_weight as (
select distinct pa.recs_model_region
    ,pa.category
    ,pa.subcategory
    ,APPROX_PERCENTILE(product_rating_count, 0.2) over(partition by recs_model_region, category, subcategory) as system_weight
from product_attributes pa),

system_attributes as (select
    b.recs_model_region
    ,b.category
    ,b.subcategory
    ,count(distinct review_id) as system_rating_count  
    ,avg(rating) as system_rating_avg  
from vistaprint.product.product_review pr
    join vistaprint.product.product_categorization pc
    on pr.product_key = pc.product_key
    left join vistaprint.transactions.lob_business_review lbr
        on pr.order_number = lbr.order_number
        and pr.order_item_id = lbr.item_id
    join vistaprint.product.product_mpv m 
        on pr.product_key = m.product_key
        and pr.country_code = m.country_code
        and coalesce(CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime), pr.created_at) >= m.valid_from
        and coalesce(CONVERT_TIMEZONE('America/New_York', 'UTC',  lbr.order_created_datetime), pr.created_at) < m.valid_to
        and m.merchant = 'VISTAPRINT'
    join dna.personalization.recommendations_base_items b
        on m.mpv_id = b.mpv_id
        and m.country_code = b.country
where published_flag = 1
and pc.category not in ('Design','Samples', 'Digital','Other')
group by 1,2,3),

system_final as (select
    sw.recs_model_region
    ,sw.category
    ,sw.subcategory
    ,sw.system_weight
    ,sa.system_rating_count
    ,sa.system_rating_avg
from
    system_prior_weight sw
    join system_attributes sa on sw.recs_model_region = sa.recs_model_region 
    and sw.category = sa.category 
    and sw.subcategory = sa.subcategory)

select s.recs_model_region
    ,s.category
    ,s.subcategory
    ,pa.product_group
    ,s.system_weight
    ,s.system_rating_count
    ,s.system_rating_avg
    ,pa.mpv_id
    ,pa.product_rating_count
    ,pa.product_rating_avg
from system_final s
join product_attributes pa
on s.recs_model_region = pa.recs_model_region
and s.category = pa.category
and s.subcategory = pa.subcategory
"""

print(ratings_proxy_query)

# COMMAND ----------

cols_to_convert = ['SYSTEM_WEIGHT','SYSTEM_RATING_AVG','PRODUCT_RATING_COUNT','PRODUCT_RATING_AVG']

ratings_df = snowflake.execute_reader(ratings_proxy_query).cache()
for col_name in cols_to_convert:
    ratings_df = ratings_df.withColumn(col_name, col(col_name).cast('float'))

# COMMAND ----------

ratings_df = ratings_df.withColumn("CALC_PRODUCT_RATING", round(bayes_avg("SYSTEM_WEIGHT", "SYSTEM_RATING_AVG", "PRODUCT_RATING_COUNT", "PRODUCT_RATING_AVG"),3))

# COMMAND ----------

window_spec = Window.partitionBy("RECS_MODEL_REGION").orderBy("CALC_PRODUCT_RATING")
ratings_df = ratings_df.withColumn("percentile", percent_rank().over(window_spec))

ratings_df = ratings_df.withColumn(
    "RATING_PROXY_CATEGORY",
    when(col("percentile") <= 0.25, '4_least')
    .when((col("percentile") > 0.25) & (col("percentile") <= 0.5), '3_low')
    .when((col("percentile") > 0.5) & (col("percentile") <= 0.75), '2_medium')
    .otherwise('1_high'))

ratings_df = ratings_df.drop("percentile")
ratings_df = ratings_df.withColumn("CREATED_AT", current_timestamp())

# COMMAND ----------

snowflake.table_from_df(
    df=ratings_df,
    database=db,
    schema=schema,
    table=f"product_ratings_proxy",
    mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Design Complexity

# COMMAND ----------

design_complexity_proxy_query = f"""
with session_starts as (
    select s.value::string as segment_visit_id,
        min(session_start_time) as session_start_time
    from VISTAPRINT.WEB_TRACKING.SITE_DATA_SESSIONS sds,
        LATERAL FLATTEN(input => PARSE_JSON(sds.segment_visit_ids)) s
    where session_start_time >= dateadd ('month', '-18', date (sysdate ()))
    group by 1
),
completed_studio_sessions as (
    select studio_unique_session,
        min(session_start_time) as session_start_time
    from "VISTAPRINT"."WEB_TRACKING_EVENTS"."STUDIO_CONTINUE" sc
        join session_starts ss on sc.visit = ss.segment_visit_id
    where label = 'Continue Button'
        and date (received_at) >= dateadd ('month', '-18', date (sysdate ()))
    group by 1
),
session_classifier as (
    select work_id,
        case
            when count(distinct studio_user_session) > 1 then 1
            else 0
        end as multiple_user_session_same_work_flag
    from vistaprint.web_tracking_events.studio_tracking
    where work_id is not null
    group by 1
),
single_session_duration as (
    select st.locale,
        st.studio_user_session,
        m.mpv_id,
        sum(st.time_since_load) as studio_duration_ms
    from vistaprint.web_tracking_events.studio_tracking st
        join completed_studio_sessions css on st.studio_unique_session = css.studio_unique_session
        join vistaprint.product.product_mpv m on st.core_product_id = m.product_key
        and st.locale = m.country_code
        and css.session_start_time >= m.valid_from
        and css.session_start_time < m.valid_to
        and m.merchant = 'VISTAPRINT'
        left join session_classifier sc on sc.work_id = st.work_id
    where st.label = 'Studio review page view'
        and coalesce(sc.multiple_user_session_same_work_flag, 0) = 0
    group by 1, 2, 3
),
multiple_session_duration as (
    select st.locale,
        st.work_id,
        m.mpv_id,
        sum(st.time_since_load) as studio_duration_ms
    from vistaprint.web_tracking_events.studio_tracking st
        join completed_studio_sessions css on st.studio_unique_session = css.studio_unique_session
        join session_classifier sc on sc.work_id = st.work_id
        join vistaprint.product.product_mpv m on st.core_product_id = m.product_key
        and st.locale = m.country_code
        and css.session_start_time >= m.valid_from
        and css.session_start_time < m.valid_to
        and m.merchant = 'VISTAPRINT'
    where st.label = 'Studio review page view'
        and sc.multiple_user_session_same_work_flag = 1
    group by 1, 2, 3
),
combined_durations as (
    select studio_user_session as combined_studio_id,
        'single' as session_type,
        mpv_id,
        round(studio_duration_ms / 60000, 1) as studio_duration_mins,
        locale
    from single_session_duration
    union all
    select work_id as combined_studio_id,
        'multiple' as session_type,
        mpv_id,
        round(studio_duration_ms / 60000, 1) as studio_duration_mins,
        locale
    from multiple_session_duration
),
product_attributes as (
    select b.recs_model_region,
        b.category,
        b.subcategory,
        b.product_group,
        cd.mpv_id,
        count(combined_studio_id) as product_design_count,
        median(studio_duration_mins) as product_duration_median
    from combined_durations cd
        join dna.personalization.recommendations_base_items b on cd.mpv_id = b.mpv_id
        and cd.locale = b.country
    where b.category not in ('Design', 'Samples', 'Digital', 'Other')
    group by 1, 2, 3, 4, 5
),
system_prior_weight as (
    select distinct pad.recs_model_region,
        pad.category,
        pad.subcategory,
        APPROX_PERCENTILE(product_design_count, 0.2) over(
            partition by recs_model_region,
            category,
            subcategory
        ) as system_weight
    from product_attributes pad
),
system_attributes as (
    select b.recs_model_region,
        b.category,
        b.subcategory,
        count(combined_studio_id) as system_design_count,
        median(studio_duration_mins) as system_duration_median
    from combined_durations cd
        join dna.personalization.recommendations_base_items b on cd.mpv_id = b.mpv_id
        and cd.locale = b.country
    where b.category not in ('Design', 'Samples', 'Digital', 'Other')
    group by 1, 2, 3
),
system_final as (
    select sw.recs_model_region,
        sw.category,
        sw.subcategory,
        sw.system_weight,
        sa.system_design_count,
        sa.system_duration_median
    from system_prior_weight sw
        join system_attributes sa on sw.recs_model_region = sa.recs_model_region
        and sw.category = sa.category
        and sw.subcategory = sa.subcategory
)
select s.recs_model_region,
    s.category,
    s.subcategory,
    pa.product_group,
    s.system_weight,
    s.system_design_count,
    s.system_duration_median,
    pa.mpv_id,
    pa.product_design_count,
    pa.product_duration_median
from system_final s
    join product_attributes pa on s.recs_model_region = pa.recs_model_region
    and s.category = pa.category
    and s.subcategory = pa.subcategory
"""

print(design_complexity_proxy_query)

# COMMAND ----------

cols_to_convert = ['SYSTEM_WEIGHT','SYSTEM_DURATION_MEDIAN','PRODUCT_DESIGN_COUNT','PRODUCT_DURATION_MEDIAN']

complexity_df = snowflake.execute_reader(design_complexity_proxy_query).cache()
for col_name in cols_to_convert:
    complexity_df = complexity_df.withColumn(col_name, col(col_name).cast('float'))

# COMMAND ----------

complexity_df = complexity_df.withColumn("CALC_STUDIO_DURATION", bayes_avg("SYSTEM_WEIGHT", "SYSTEM_DURATION_MEDIAN", "PRODUCT_DESIGN_COUNT", "PRODUCT_DURATION_MEDIAN"))

# COMMAND ----------

window_spec = Window.partitionBy("RECS_MODEL_REGION").orderBy("CALC_STUDIO_DURATION")
complexity_df = complexity_df.withColumn("percentile", percent_rank().over(window_spec))

complexity_df = complexity_df.withColumn(
    "DESIGN_COMPLEXITY_PROXY_CATEGORY",
    when(col("percentile") <= 0.25, '4_least')
    .when((col("percentile") > 0.25) & (col("percentile") <= 0.5), '3_low')
    .when((col("percentile") > 0.5) & (col("percentile") <= 0.75), '2_medium')
    .otherwise('1_high')
)

complexity_df = complexity_df.drop("percentile")
complexity_df = complexity_df.withColumn("CREATED_AT", current_timestamp())

# COMMAND ----------

snowflake.table_from_df(
    df=complexity_df,
    database=db,
    schema=schema,
    table=f"product_design_complexity_proxy",
    mode="overwrite")
