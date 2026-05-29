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
    dynamo_items_table = "industry_popular_items_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
    dynamo_items_table = "industry_popular_items_dev"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
    dynamo_items_table = "industry_popular_items_prd"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Viewed
# MAGIC #### Step 1: Region X Product
# MAGIC ###### Percent = Product Viewed User Count in Region / Overall User Count in Region
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_viewed_by_region_product = f"""
create or replace table {db}.{schema}.ipi_viewed_by_region_product as
with mpv_details as (
    select
        mpv.mpv_id,
        mpv.product_key,
        case
            when country_code in ('FR', 'BE', 'NL') then 'FRCEN'
            when country_code in ('AU', 'NZ', 'SG') then 'ANZS'
            when country_code in ('DE', 'AT', 'CH') then 'DACH'
            when country_code in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when country_code in ('GB', 'IE') then 'UK'
            else country_code
        end as locale_region
    from
        VISTAPRINT.PRODUCT.PRODUCT_MPV mpv
    group by
        all
),
region_count as (
    select
        case
            when locale in ('FR', 'BE', 'NL') then 'FRCEN'
            when locale in ('AU', 'NZ', 'SG') then 'ANZS'
            when locale in ('DE', 'AT', 'CH') then 'DACH'
            when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when locale in ('GB', 'IE') then 'UK'
            else locale
        end as locale_region,
        count(distinct pv.user_id) as user_count_region
    from
        VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED pv
        left join vistaprint.shopper.customer_traits ct on pv.user_id = ct.canonical_id
        left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
    where
        pv.user_id is not null
        and pv.page_section = 'Product Page'
        and pv.original_timestamp >= DATEADD(day, -91, GETDATE())
        and cd.industry_l1_confidence_rank_category <= 8
    group by
        locale_region
    order by
        user_count_region desc
),
region_product_count as (
    select
        case
            when locale in ('FR', 'BE', 'NL') then 'FRCEN'
            when locale in ('AU', 'NZ', 'SG') then 'ANZS'
            when locale in ('DE', 'AT', 'CH') then 'DACH'
            when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when locale in ('GB', 'IE') then 'UK'
            else locale
        end as locale_region,
        pv.product_id,
        count(distinct pv.user_id) as user_count_region_product
    from
        VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED pv
        left join vistaprint.shopper.customer_traits ct on pv.user_id = ct.canonical_id
        left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
    where
        pv.user_id is not null
        and pv.page_section = 'Product Page'
        and pv.original_timestamp >= DATEADD(day, -91, GETDATE())
        and cd.industry_l1_confidence_rank_category <= 8
    group by
        locale_region,
        pv.product_id
    order by
        locale_region,
        user_count_region_product desc
)
select
    rpc.locale_region,
    rpc.product_id,
    mpv.product_key,
    rc.user_count_region,
    rpc.user_count_region_product,
    round(
        rpc.user_count_region_product / rc.user_count_region,
        2
    ) as percent
from
    region_product_count rpc
    left join region_count rc on rc.locale_region = rpc.locale_region
    left join mpv_details mpv on rpc.product_id = mpv.mpv_id
    and rpc.locale_region = mpv.locale_region
order by
    rpc.locale_region,
    percent desc
"""
print(ipi_viewed_by_region_product)
snowflake.execute_nonquery(ipi_viewed_by_region_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Viewed
# MAGIC #### Step 2: Region X Industry X Product
# MAGIC ###### Percent = Product Viewed User Count in Region X Industry / Overall User Count in Region X Industry
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_viewed_by_region_industry_product = f"""
create or replace table {db}.{schema}.ipi_viewed_by_region_industry_product as
with mpv_details as (
        select
            mpv.mpv_id,
            mpv.product_key,
            case
                when country_code in ('FR', 'BE', 'NL') then 'FRCEN'
                when country_code in ('AU', 'NZ', 'SG') then 'ANZS'
                when country_code in ('DE', 'AT', 'CH') then 'DACH'
                when country_code in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when country_code in ('GB', 'IE') then 'UK'
                else country_code
            end as locale_region
        from
            VISTAPRINT.PRODUCT.PRODUCT_MPV mpv
        group by
            all
    ),
    region_industry_count as (
        select
            case
                when locale in ('FR', 'BE', 'NL') then 'FRCEN'
                when locale in ('AU', 'NZ', 'SG') then 'ANZS'
                when locale in ('DE', 'AT', 'CH') then 'DACH'
                when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when locale in ('GB', 'IE') then 'UK'
                else locale
            end as locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            count(distinct pv.user_id) as user_count_region_industry
        from
            VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED pv
            left join vistaprint.shopper.customer_traits ct on pv.user_id = ct.canonical_id
            left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
        where
            pv.user_id is not null
            and pv.page_section = 'Product Page'
            and pv.original_timestamp >= DATEADD(day, -91, GETDATE())
            and cd.industry_l1_confidence_rank_category <= 8
        group by
            locale_region,
            cd.industry_l1,
            cd.industry_l1_id
        order by
            user_count_region_industry desc
    ),
    region_industry_product_count as (
        select
            case
                when locale in ('FR', 'BE', 'NL') then 'FRCEN'
                when locale in ('AU', 'NZ', 'SG') then 'ANZS'
                when locale in ('DE', 'AT', 'CH') then 'DACH'
                when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when locale in ('GB', 'IE') then 'UK'
                else locale
            end as locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            pv.product_id,
            count(distinct pv.user_id) as user_count_region_industry_product
        from
            VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED pv
            left join vistaprint.shopper.customer_traits ct on pv.user_id = ct.canonical_id
            left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
        where
            pv.user_id is not null
            and pv.page_section = 'Product Page'
            and pv.original_timestamp >= DATEADD(day, -91, GETDATE())
            and cd.industry_l1_confidence_rank_category <= 8
        group by
            locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            pv.product_id
        order by
            locale_region,
            user_count_region_industry_product desc
    )
select
    rpc.locale_region,
    rpc.industry_l1,
    rpc.industry_l1_id,
    rpc.product_id,
    mpv.product_key,
    rc.user_count_region_industry,
    rpc.user_count_region_industry_product,
    round(
        rpc.user_count_region_industry_product / rc.user_count_region_industry,
        2
    ) as percent
from
    region_industry_product_count rpc
    left join region_industry_count rc on rc.locale_region = rpc.locale_region
    and rc.industry_l1 = rpc.industry_l1
    left join mpv_details mpv on rpc.product_id = mpv.mpv_id
    and rpc.locale_region = mpv.locale_region
order by
    rpc.locale_region,
    rpc.industry_l1,
    percent desc
"""
print(ipi_viewed_by_region_industry_product)
snowflake.execute_nonquery(ipi_viewed_by_region_industry_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Viewed
# MAGIC #### Step 3: Calculated Index
# MAGIC ###### Calculated Index = (Step 2 / Step 1 - 1) * 100

# COMMAND ----------

ipi_viewed_calculated_index = f"""
create or replace table {db}.{schema}.ipi_viewed_calculated_index as
select
    s2.locale_region,
    s2.industry_l1,
    s2.industry_l1_id,
    s2.product_id,
    s2.product_key,
    s1.user_count_region,
    s1.user_count_region_product,
    s2.user_count_region_industry,
    s2.user_count_region_industry_product,
    s2.percent as percent_step2,
    s1.percent as percent_step1,
    round(
        (
            ((percent_step2 / NULLIF(percent_step1, 0)) - 1) * 100
        ),
        2
    ) as calculated_index
from
    {db}.{schema}.ipi_viewed_by_region_industry_product s2
    left join {db}.{schema}.ipi_viewed_by_region_product s1 on s1.locale_region = s2.locale_region
    and s1.product_id = s2.product_id
    and s1.product_key = s2.product_key
where
    calculated_index is not null
    and s2.product_key is not null
    and s1.product_key is not null
order by
    s2.locale_region desc,
    s2.industry_l1,
    calculated_index desc,
    s2.user_count_region_industry_product desc
"""
print(ipi_viewed_calculated_index)
snowflake.execute_nonquery(ipi_viewed_calculated_index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Added to Cart
# MAGIC #### Step 1: Region X Product
# MAGIC ###### Percent = Product Added to Cart User Count in Region / Overall User Count in Region
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_added_to_cart_by_region_product = f"""
create or replace table {db}.{schema}.ipi_added_to_cart_by_region_product as
with mpv_details as (
    select
        mpv.mpv_id,
        mpv.product_key,
        case
            when country_code in ('FR', 'BE', 'NL') then 'FRCEN'
            when country_code in ('AU', 'NZ', 'SG') then 'ANZS'
            when country_code in ('DE', 'AT', 'CH') then 'DACH'
            when country_code in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when country_code in ('GB', 'IE') then 'UK'
            else country_code
        end as locale_region
    from
        VISTAPRINT.PRODUCT.PRODUCT_MPV mpv
    group by
        all
),
region_count as (
    select
        case
            when locale in ('FR', 'BE', 'NL') then 'FRCEN'
            when locale in ('AU', 'NZ', 'SG') then 'ANZS'
            when locale in ('DE', 'AT', 'CH') then 'DACH'
            when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when locale in ('GB', 'IE') then 'UK'
            else locale
        end as locale_region,
        count(distinct pa.user_id) as user_count_region
    from
        VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED pa
        left join vistaprint.shopper.customer_traits ct on pa.user_id = ct.canonical_id
        left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
    where
        pa.user_id is not null
        and pa.original_timestamp >= DATEADD(day, -91, GETDATE())
        and cd.industry_l1_confidence_rank_category <= 8
    group by
        locale_region
    order by
        user_count_region desc
),
region_product_count as (
    select
        case
            when locale in ('FR', 'BE', 'NL') then 'FRCEN'
            when locale in ('AU', 'NZ', 'SG') then 'ANZS'
            when locale in ('DE', 'AT', 'CH') then 'DACH'
            when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when locale in ('GB', 'IE') then 'UK'
            else locale
        end as locale_region,
        pa.product_id,
        count(distinct pa.user_id) as user_count_region_product
    from
        VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED pa
        left join vistaprint.shopper.customer_traits ct on pa.user_id = ct.canonical_id
        left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
    where
        pa.user_id is not null
        and pa.original_timestamp >= DATEADD(day, -91, GETDATE())
        and cd.industry_l1_confidence_rank_category <= 8
    group by
        locale_region,
        pa.product_id
    order by
        locale_region,
        user_count_region_product desc
)
select
    rpc.locale_region,
    rpc.product_id,
    mpv.product_key,
    rc.user_count_region,
    rpc.user_count_region_product,
    round(
        rpc.user_count_region_product / rc.user_count_region,
        2
    ) as percent
from
    region_product_count rpc
    left join region_count rc on rc.locale_region = rpc.locale_region
    left join mpv_details mpv on rpc.product_id = mpv.mpv_id
    and rpc.locale_region = mpv.locale_region
order by
    rpc.locale_region,
    percent desc
"""
print(ipi_added_to_cart_by_region_product)
snowflake.execute_nonquery(ipi_added_to_cart_by_region_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Added to Cart
# MAGIC #### Step 2: Region X Industry X Product
# MAGIC ###### Percent = Product Added to Cart User Count in Region X Industry / Overall User Count in Region X Industry
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_added_to_cart_by_region_industry_product = f"""
create or replace table {db}.{schema}.ipi_added_to_cart_by_region_industry_product as
with mpv_details as (
    select
        mpv.mpv_id,
        mpv.product_key,
        case
            when country_code in ('FR', 'BE', 'NL') then 'FRCEN'
            when country_code in ('AU', 'NZ', 'SG') then 'ANZS'
            when country_code in ('DE', 'AT', 'CH') then 'DACH'
            when country_code in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when country_code in ('GB', 'IE') then 'UK'
            else country_code
        end as locale_region
    from
        VISTAPRINT.PRODUCT.PRODUCT_MPV mpv
    group by
        all
),    
    region_industry_count as (
        select
            case
                when locale in ('FR', 'BE', 'NL') then 'FRCEN'
                when locale in ('AU', 'NZ', 'SG') then 'ANZS'
                when locale in ('DE', 'AT', 'CH') then 'DACH'
                when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when locale in ('GB', 'IE') then 'UK'
                else locale
            end as locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            count(distinct pa.user_id) as user_count_region_industry
        from
            VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED pa
            left join vistaprint.shopper.customer_traits ct on pa.user_id = ct.canonical_id
            left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
        where
            pa.user_id is not null
            and pa.original_timestamp >= DATEADD(day, -91, GETDATE())
            and cd.industry_l1_confidence_rank_category <= 8
        group by
            locale_region,
            cd.industry_l1,
            cd.industry_l1_id
        order by
            user_count_region_industry desc
    ),
    region_industry_product_count as (
        select
            case
                when locale in ('FR', 'BE', 'NL') then 'FRCEN'
                when locale in ('AU', 'NZ', 'SG') then 'ANZS'
                when locale in ('DE', 'AT', 'CH') then 'DACH'
                when locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when locale in ('GB', 'IE') then 'UK'
                else locale
            end as locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            pa.product_id,
            count(distinct pa.user_id) as user_count_region_industry_product
        from
            VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED pa
            left join vistaprint.shopper.customer_traits ct on pa.user_id = ct.canonical_id
            left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
        where
            pa.user_id is not null
            and pa.original_timestamp >= DATEADD(day, -91, GETDATE())
            and cd.industry_l1_confidence_rank_category <= 8
        group by
            locale_region,
            cd.industry_l1,
            cd.industry_l1_id,
            pa.product_id
        order by
            locale_region,
            user_count_region_industry_product desc
    )
select
    rpc.locale_region,
    rpc.industry_l1,
    rpc.industry_l1_id,
    rpc.product_id,
    mpv.product_key,
    rc.user_count_region_industry,
    rpc.user_count_region_industry_product,
    round(
        rpc.user_count_region_industry_product / rc.user_count_region_industry,
        2
    ) as percent
from
    region_industry_product_count rpc
    left join region_industry_count rc on rc.locale_region = rpc.locale_region
    and rc.industry_l1 = rpc.industry_l1
    left join mpv_details mpv on rpc.product_id = mpv.mpv_id
    and rpc.locale_region = mpv.locale_region
order by
    rpc.locale_region,
    rpc.industry_l1,
    percent desc
"""
print(ipi_added_to_cart_by_region_industry_product)
snowflake.execute_nonquery(ipi_added_to_cart_by_region_industry_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Added to Cart
# MAGIC #### Step 3: Calculated Index
# MAGIC ###### Calculated Index = (Step 2 / Step 1 - 1) * 100

# COMMAND ----------

ipi_added_to_cart_calculated_index = f"""
create or replace table {db}.{schema}.ipi_added_to_cart_calculated_index as
select
    s2.locale_region,
    s2.industry_l1,
    s2.industry_l1_id,
    s2.product_id,
    s2.product_key,
    s1.user_count_region,
    s1.user_count_region_product,
    s2.user_count_region_industry,
    s2.user_count_region_industry_product,
    s2.percent as percent_step2,
    s1.percent as percent_step1,
    round(
        (
            ((percent_step2 / NULLIF(percent_step1, 0)) - 1) * 100
        ),
        2
    ) as calculated_index
from
    {db}.{schema}.ipi_added_to_cart_by_region_industry_product s2
    left join {db}.{schema}.ipi_added_to_cart_by_region_product s1 on s1.locale_region = s2.locale_region
    and s1.product_id = s2.product_id
    and s1.product_key = s2.product_key
where
    calculated_index is not null
    and s2.product_key is not null
    and s1.product_key is not null
order by
    s2.locale_region desc,
    s2.industry_l1,
    calculated_index desc,
    s2.user_count_region_industry_product desc
"""
print(ipi_added_to_cart_calculated_index)
snowflake.execute_nonquery(ipi_added_to_cart_calculated_index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Order Completed
# MAGIC #### Step 1: Region X Product
# MAGIC ###### Percent = Product Order Completed User Count in Region / Overall User Count in Region
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_order_completed_by_region_product = f"""
create or replace table {db}.{schema}.ipi_order_completed_by_region_product as
with order_details as (
    select
        oc.user_id,
        oc.original_timestamp,
        oc.locale,
        case
            when oc.locale in ('FR', 'BE', 'NL') then 'FRCEN'
            when oc.locale in ('AU', 'NZ', 'SG') then 'ANZS'
            when oc.locale in ('DE', 'AT', 'CH') then 'DACH'
            when oc.locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when oc.locale in ('GB', 'IE') then 'UK'
            else oc.locale
        end as locale_region,
        oc.order_id,
        lob.order_number,
        lob.product_key as order_product_key,
        mpv.product_key,
        mpv.mpv_id,
        cd.industry_l1
    from
        VISTAPRINT.WEB_TRACKING_EVENTS.ORDER_COMPLETED oc
        left join vistaprint.shopper.customer_traits ct on oc.user_id = ct.canonical_id
        left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
        join vistaprint.transactions.lob_business_review lob on lob.order_number = oc.order_id
        join VISTAPRINT.PRODUCT.PRODUCT_MPV mpv on lob.product_key = mpv.product_key
    where
        oc.user_id is not null
        and oc.original_timestamp >= DATEADD(day, -91, GETDATE())
        and cd.industry_l1_confidence_rank_category <= 8
    group by
        all
    order by
        order_id
),
region_order_details as (
    select
        locale_region,
        count (distinct user_id) as user_count_region
    from
        order_details
    group by
        locale_region
    order by
        user_count_region desc
),
region_product_order_details as (
    select
        locale_region,
        mpv_id as product_id,
        order_product_key as product_key,
        count (distinct user_id) as user_count_region_product
    from
        order_details
    group by
        locale_region,
        product_id,
        order_product_key
    order by
        user_count_region_product desc
)
select
    rpc.locale_region,
    rpc.product_id,
    rpc.product_key,
    rc.user_count_region,
    rpc.user_count_region_product,
    round(
        rpc.user_count_region_product / rc.user_count_region,
        2
    ) as percent
from
    region_product_order_details rpc
    left join region_order_details rc on rc.locale_region = rpc.locale_region
order by
    rpc.locale_region,
    percent desc
"""
print(ipi_order_completed_by_region_product)
snowflake.execute_nonquery(ipi_order_completed_by_region_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Order Completed
# MAGIC #### Step 2: Region X Industry X Product
# MAGIC ###### Percent = Product Order Completed User Count in Region X Industry / Overall User Count in Region X Industry
# MAGIC - Logged in users (user_id not null)
# MAGIC - In the last 91 days
# MAGIC - For industries with confidence rank category 1 to 8

# COMMAND ----------

ipi_order_completed_by_region_industry_product = f"""
create or replace table {db}.{schema}.ipi_order_completed_by_region_industry_product as
    with order_details as (
        select
            oc.user_id,
            oc.original_timestamp,
            oc.locale,
            case
                when oc.locale in ('FR', 'BE', 'NL') then 'FRCEN'
                when oc.locale in ('AU', 'NZ', 'SG') then 'ANZS'
                when oc.locale in ('DE', 'AT', 'CH') then 'DACH'
                when oc.locale in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
                when oc.locale in ('GB', 'IE') then 'UK'
                else oc.locale
            end as locale_region,
            oc.order_id,
            lob.order_number,
            lob.product_key as order_product_key,
            mpv.product_key,
            mpv.mpv_id,
            cd.industry_l1,
            cd.industry_l1_id,
        from
            VISTAPRINT.WEB_TRACKING_EVENTS.ORDER_COMPLETED oc
            left join vistaprint.shopper.customer_traits ct on oc.user_id = ct.canonical_id
            left join vistaprint.shopper.customer_industry_vertical cd on ct.customer_id = cd.customer_id
            join vistaprint.transactions.lob_business_review lob on lob.order_number = oc.order_id
            join VISTAPRINT.PRODUCT.PRODUCT_MPV mpv on lob.product_key = mpv.product_key
        where
            oc.user_id is not null
            and oc.original_timestamp >= DATEADD(day, -91, GETDATE())
            and cd.industry_l1_confidence_rank_category <= 8
        group by
            all
        order by
            order_id
    ),
    region_industry_order_details as (
        select
            locale_region,
            industry_l1,
            industry_l1_id,
            count (distinct user_id) as user_count_region_industry
        from
            order_details
        group by
            locale_region,
            industry_l1,
            industry_l1_id
        order by
            locale_region,
            user_count_region_industry desc
    ),
    region_industry_product_order_details as (
        select
            locale_region,
            industry_l1,
            industry_l1_id,
            mpv_id as product_id,
            order_product_key as product_key,
            count (distinct user_id) as user_count_region_industry_product
        from
            order_details
        group by
            locale_region,
            industry_l1,
            industry_l1_id,
            product_id,
            order_product_key
        order by
            locale_region,
            industry_l1,
            user_count_region_industry_product desc
    )
select
    rpc.locale_region,
    rpc.industry_l1,
    rpc.industry_l1_id,
    rpc.product_id,
    rpc.product_key,
    rc.user_count_region_industry,
    rpc.user_count_region_industry_product,
    round(
        rpc.user_count_region_industry_product / rc.user_count_region_industry,
        2
    ) as percent
from
    region_industry_product_order_details rpc
    left join region_industry_order_details rc on rc.locale_region = rpc.locale_region
    and rc.industry_l1 = rpc.industry_l1
order by
    rpc.locale_region,
    rpc.industry_l1,
    percent desc
"""
print(ipi_order_completed_by_region_industry_product)
snowflake.execute_nonquery(ipi_order_completed_by_region_industry_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Order Completed
# MAGIC #### Step 3: Calculated Index
# MAGIC ###### Calculated Index = (Step 2 / Step 1 - 1) * 100

# COMMAND ----------

ipi_order_completed_calculated_index = f"""
create or replace table {db}.{schema}.ipi_order_completed_calculated_index as
select
    s2.locale_region,
    s2.industry_l1,
    s2.industry_l1_id,
    s2.product_id,
    s2.product_key,
    s1.user_count_region,
    s1.user_count_region_product,
    s2.user_count_region_industry,
    s2.user_count_region_industry_product,
    s2.percent as percent_step2,
    s1.percent as percent_step1,
    round(
        (
            ((percent_step2 / NULLIF(percent_step1, 0)) - 1) * 100
        ),
        2
    ) as calculated_index
from
    {db}.{schema}.ipi_order_completed_by_region_industry_product s2
    left join {db}.{schema}.ipi_order_completed_by_region_product s1 on s1.locale_region = s2.locale_region
    and s1.product_id = s2.product_id
where
    calculated_index is not null
    and s2.product_key is not null
    and s1.product_key is not null
order by
    s2.locale_region desc,
    s2.industry_l1,
    calculated_index desc,
    s2.user_count_region_industry_product desc
"""
print(ipi_order_completed_calculated_index)
snowflake.execute_nonquery(ipi_order_completed_calculated_index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Top 20 Products by Region X Industry
# MAGIC ###### Final index weights: 
# MAGIC - Product Order Completed = 2
# MAGIC - Product Added to Cart = 1.5
# MAGIC - Product Viewed = 1
# MAGIC
# MAGIC **Final Index** = _(2 * Product Order Completed.calculated_index) + (1.5 * Product Added to Cart.calculated_index) + (1 * Product Viewed.calculated_index)_
# MAGIC
# MAGIC Top 20 products ranking based on **_descending order of Final index followed by Product Order Completed User Count in Region X Industry_**
# MAGIC
# MAGIC ###### Data checks: 
# MAGIC - Product id is not blank/null 
# MAGIC - Numerical product_id doesn't show up – 8/6 etc. 
# MAGIC - Product id is not product key or does not start with “PRD-” 
# MAGIC - To ensure that there are no reoccurrences the same products - Top few products are not from the same category (For Business Cards) and product group (For rest of the products) 
# MAGIC - Exclude missing products/discontinued products 
# MAGIC - Product is not under indexed, index is not negative

# COMMAND ----------

ipi_top_industry_products = f"""
create or replace table {db}.{schema}.ipi_top_industry_products as
with mpv_details as (
    select
        mpv.mpv_id,
        mpv.product_key,
        pc.category,
        pc.subcategory,
        pc.product_group,
        case
            when country_code in ('FR', 'BE', 'NL') then 'FRCEN'
            when country_code in ('AU', 'NZ', 'SG') then 'ANZS'
            when country_code in ('DE', 'AT', 'CH') then 'DACH'
            when country_code in ('ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO') then 'EU'
            when country_code in ('GB', 'IE') then 'UK'
            else country_code
        end as locale_region,
        mpv.valid_from,
        mpv.valid_to
    from
        VISTAPRINT.PRODUCT.PRODUCT_MPV mpv
        left join VISTAPRINT.PRODUCT.PRODUCT_CATEGORIZATION pc on mpv.product_key = pc.product_key
    where
        current_date <= date(mpv.valid_to)
    group by
        all
),
region_industry_product as (
    select
        upper(rm.recs_model_region) as locale_region,
        iv.industry_l1,
        iv.industry_l1_id,
        pm.mpv_id as product_id,
        pm.product_key,
        pm.category,
        pm.subcategory,
        pm.product_group,
        pm.locale_region as product_region
    from
        dna.personalization.recs_country_region_mapping rm
        left join mpv_details pm on (upper(rm.recs_model_region)) = pm.locale_region
        left join vistaprint.shopper.customer_industry_vertical iv on 1 = 1
    group by
        all
),
data_checks as (
    select
        rip.locale_region,
        rip.industry_l1,
        rip.industry_l1_id,
        rip.product_id,
        rip.product_key,
        rip.category,
        rip.subcategory,
        rip.product_group,
        po.user_count_region_industry_product,
        pv.calculated_index as product_viewed_calculated_index,
        pa.calculated_index as product_added_to_cart_calculated_index,
        po.calculated_index as product_order_completed_calculated_index,
        (
            (2 * po.calculated_index) + (1.5 * pa.calculated_index) + (1 * pv.calculated_index)
        ) as final_index,
        case
            when rip.category = 'Business Cards' then row_number() over (
                partition by rip.locale_region,
                rip.industry_l1,
                rip.category
                order by
                    final_index desc,
                    po.user_count_region_industry_product desc
            )
            else row_number() over (
                partition by rip.locale_region,
                rip.industry_l1,
                rip.product_group
                order by
                    final_index desc,
                    po.user_count_region_industry_product desc
            )
        end as r_no_product
    from
        region_industry_product rip
        left join {db}.{schema}.ipi_viewed_calculated_index pv on rip.locale_region = pv.locale_region
        and rip.industry_l1 = pv.industry_l1
        and rip.industry_l1_id = pv.industry_l1_id
        and rip.product_id = pv.product_id
        left join {db}.{schema}.ipi_added_to_cart_calculated_index pa on rip.locale_region = pa.locale_region
        and rip.industry_l1 = pa.industry_l1
        and rip.industry_l1_id = pa.industry_l1_id
        and rip.product_id = pa.product_id
        left join {db}.{schema}.ipi_order_completed_calculated_index po on rip.locale_region = po.locale_region
        and rip.industry_l1 = po.industry_l1
        and rip.industry_l1_id = po.industry_l1_id
        and rip.product_id = po.product_id
    where 1=1
        and final_index is not null
        and final_index >= 0
        and rip.product_id is not null
        and rip.product_id not like 'PRD-%'
        and try_to_numeric(rip.product_id) is null
    order by
        rip.locale_region,
        rip.industry_l1,
        final_index desc,
        po.user_count_region_industry_product desc
),
top_20_products as (
    select
        *,
        ROW_NUMBER() over (
            PARTITION BY locale_region,
            industry_l1
            order by
                final_index desc,
                user_count_region_industry_product desc
        ) AS r_no
    from
        data_checks
    where
        r_no_product = 1
)
select
    locale_region,
    industry_l1,
    industry_l1_id,
    product_id,
    r_no
from
    top_20_products
where
    r_no <= 20 and (locale_region, product_id) in (
        select upper(recs_model_region), mpv_id
        from dna.personalization.recommendations_base_items
    )
"""
print(ipi_top_industry_products)
snowflake.execute_nonquery(ipi_top_industry_products)

# COMMAND ----------

popular_items_for_dynamo = f"""
with
items_locale_list as (
  select
    industry_l1_id as industry_id,
    case
      when locale_region = 'FRCEN' then ['FR', 'BE', 'NL']
      when locale_region = 'ANZS' then ['AU', 'NZ', 'SG']
      when locale_region = 'DACH' then ['DE', 'AT', 'CH']
      when locale_region = 'EU' then ['ES', 'FI', 'SE', 'IT', 'DK', 'PT', 'NO']
      when locale_region = 'UK' then ['GB', 'IE']
      when locale_region = 'US' then ['US']
      when locale_region = 'CA' then ['CA']
      when locale_region = 'IN' then ['IN']
    end as locale_list,
    product_id,
    r_no
  from
    {db}.{schema}.ipi_top_industry_products
),
items_by_locale as (
  select
    ll.value::string as locale,
    industry_id,
    product_id,
    r_no,
  from
    items_locale_list,
    lateral flatten(items_locale_list.locale_list) ll
)
select
  industry_id,
  locale,
  listagg(product_id, ',') within group (order by r_no) as items
from
  items_by_locale
group by
  industry_id,
  locale
"""
print(popular_items_for_dynamo)
popular_items = snowflake.execute_reader(popular_items_for_dynamo).cache()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ###### QA checks:
# MAGIC - Number of industries should be X
# MAGIC - Number of locales should be 20
# MAGIC - Not less than 5 items per industry and locale

# COMMAND ----------


distinct_industry_id_n = popular_items.select("industry_id").distinct().count()
assert distinct_industry_id_n == 20, f"Expected 20 unique industry IDs, got {distinct_industry_id_n}"

distinct_locales_n = popular_items.select("locale").distinct().count()
assert distinct_locales_n == 21, f"Expected 21 unique locales, got {distinct_locales_n}"

too_few_items = popular_items.where("size(split(items, ',')) < 5")
if not too_few_items.isEmpty():
    too_few_items.display()
    raise Exception("Found industries and locales with too few items")

# COMMAND ----------

dynamo = session.resource('dynamodb', region_name="eu-west-1")
popular_items_table = dynamo.Table(dynamo_items_table)

with popular_items_table.batch_writer() as batch:
    for row in popular_items.collect():
        batch.put_item(Item={
            "industry_id": row["INDUSTRY_ID"],
            "locale": row["LOCALE"],
            "items": row["ITEMS"]})
