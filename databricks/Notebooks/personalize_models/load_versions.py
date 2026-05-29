# Databricks notebook source
# MAGIC %md #### v6

# COMMAND ----------

# DBTITLE 1,v6_items
v6_items_schema = {
    "type": "record",
    "name": "Items",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {
            "name": "CATEGORY",
            "type": "string",
            "categorical": True
        },
        {
            "name": "SUBCATEGORY",
            "type": "string",
            "categorical": True
        },
        {
            "name": "PRODUCT_GROUP",
            "type": "string",
            "categorical": True
        },
        {
            "name": "ITEM_ID",
            "type": "string"
        },
        {
            "name": "PARENT_SITE_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "SITE_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "IS_ACCESSORY",
            "type": "int"
        },
        {
            "name": "IS_LOGOMAKER_ENABLED",
            "type": "int"
        },
        {
            "name": "DESIGN_COMPLEXITY_PROXY_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "MEDIAN_STUDIO_DURATION",
            "type": [
                "float",
                "null"
            ]
        },
        {
            "name": "BULK_PROXY_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "MEDIAN_ORDER_QUANTITY",
            "type": [
                "float",
                "null"
            ]
        },
        {
            "name": "RATING_PROXY_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "CALC_PRODUCT_RATING",
            "type": [
                "float",
                "null"
            ]
        },
        {
            "name": "HIGH_VALUE_PROXY_CATEGORY",
            "type": [
                "string",
                "null"
            ],
            "categorical": True
        },
        {
            "name": "DEFAULT_MOQ",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "MIN_UNIT_PRICE",
            "type": [
                "float",
                "null"
            ]
        },
        {
            "name": "DAYS_SINCE_MERCHANDISABLE",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "TIMESTAMP",
            "type": "long"
        }

    ],
    "version": "1.0"
}

v6_items_query = """
WITH
    REGION_BASE_ITEMS AS (
        SELECT
            CATEGORY,
            SUBCATEGORY,
            PRODUCT_GROUP,
            MPV_ID AS ITEM_ID,
            PRODUCT_KEY,
            IS_ACCESSORY,
            -- MAX FUNCTION USED TO REMOVE SITE_CATEGORY NULL VALUES
            MAX(PARENT_SITE_CATEGORY) AS PARENT_SITE_CATEGORY,
            MAX(SITE_CATEGORY) AS SITE_CATEGORY,
            -- MAX FUNCTION USED TO ENSURE SINGLE IS_LOGOMAKER_ENABLED VALUE PER ITEM IN REGION
            -- SOMETIMES THERE ARE MULTIPLE IS_LOGOMAKER_ENABLED VALUES FOR THE SAME ITEM IN DIFFERENT COUNTRIES
            MAX(IS_LOGOMAKER_ENABLED) AS IS_LOGOMAKER_ENABLED,
            MAX(DAYS_SINCE_MERCHANDISABLE) AS DAYS_SINCE_MERCHANDISABLE,
            MIN(CREATION_TIMESTAMP) AS TIMESTAMP,
        FROM
            {sf_db}.{sf_schema}.RECOMMENDATIONS_BASE_ITEMS
        WHERE
            RECS_MODEL_REGION = '{region}'
        GROUP BY ALL
    ),
    system_design_complexity_proxy as (
        select distinct
            category,
            subcategory,
            system_duration_median,
        from
            {sf_db}.{sf_schema}.PRODUCT_DESIGN_COMPLEXITY_PROXY
        where
            recs_model_region = '{region}'
    ),
    system_bulk_proxy as (
        select distinct
            category,
            subcategory,
            product_group,
            system_quant_med,
        from
            {sf_db}.{sf_schema}.PRODUCT_BULK_PROXY
        where
            recs_model_region = '{region}'
    ),
    system_rating_proxy as (
        select distinct
            category,
            subcategory,
            system_rating_avg,
        from
            {sf_db}.{sf_schema}.PRODUCT_RATINGS_PROXY
        where
            recs_model_region = '{region}'
    ),
    system_high_value_proxy as (
        select distinct
            category,
            subcategory,
            median_subcategory_price,
        from
            {sf_db}.{sf_schema}.PRODUCT_HIGH_VALUE_PROXY
        where
            recs_model_region = '{region}'
    ),
    FEATURES AS (
        SELECT
            B.ITEM_ID,
            coalesce(HV.HIGH_VALUE_PROXY_CATEGORY, '5_unknown') as HIGH_VALUE_PROXY_CATEGORY,
            HV.DEFAULT_MOQ,
            coalesce(HV.CALC_MOQ_UNIT_PRICE_USD, system_vp.median_subcategory_price) AS MIN_UNIT_PRICE,
            coalesce(BP.BULK_PROXY_CATEGORY, '5_unknown') as BULK_PROXY_CATEGORY,
            coalesce(BP.CALC_MEDIAN_QUANTITY, system_bp.system_quant_med) as MEDIAN_ORDER_QUANTITY,
            coalesce(R.RATING_PROXY_CATEGORY, '5_unknown') as RATING_PROXY_CATEGORY,
            coalesce(R.CALC_PRODUCT_RATING, system_rp.system_rating_avg) as CALC_PRODUCT_RATING,
            coalesce(DC.DESIGN_COMPLEXITY_PROXY_CATEGORY, '5_unknown') as DESIGN_COMPLEXITY_PROXY_CATEGORY,
            coalesce(DC.product_duration_median, system_cp.system_duration_median) as MEDIAN_STUDIO_DURATION,
        FROM
            REGION_BASE_ITEMS b
            LEFT JOIN {sf_db}.{sf_schema}.PRODUCT_HIGH_VALUE_PROXY HV ON B.ITEM_ID = HV.MPV_ID
            AND hv.recs_model_region = '{region}'
            LEFT JOIN system_high_value_proxy system_vp on b.category = system_vp.category
            AND b.subcategory = system_vp.subcategory
            LEFT JOIN {sf_db}.{sf_schema}.PRODUCT_BULK_PROXY BP ON B.ITEM_ID = BP.MPV_ID
            AND bp.recs_model_region = '{region}'
            LEFT JOIN system_bulk_proxy system_bp on b.category = system_bp.category
            AND b.subcategory = system_bp.subcategory
            AND b.product_group = system_bp.product_group
            LEFT JOIN {sf_db}.{sf_schema}.PRODUCT_RATINGS_PROXY R ON B.ITEM_ID = R.MPV_ID
            AND r.recs_model_region = '{region}'
            LEFT JOIN system_rating_proxy system_rp on b.category = system_rp.category
            AND b.subcategory = system_rp.subcategory
            LEFT JOIN {sf_db}.{sf_schema}.PRODUCT_DESIGN_COMPLEXITY_PROXY DC ON B.ITEM_ID = DC.MPV_ID
            AND DC.recs_model_region = '{region}'
            LEFT JOIN system_design_complexity_proxy system_cp on b.category = system_cp.category
            AND b.subcategory = system_cp.subcategory
    )
SELECT
    CATEGORY,
    SUBCATEGORY,
    PRODUCT_GROUP,
    B.ITEM_ID,
    PARENT_SITE_CATEGORY,
    SITE_CATEGORY,
    IS_ACCESSORY,
    IS_LOGOMAKER_ENABLED,
    DESIGN_COMPLEXITY_PROXY_CATEGORY,
    MEDIAN_STUDIO_DURATION,
    BULK_PROXY_CATEGORY,
    MEDIAN_ORDER_QUANTITY,
    RATING_PROXY_CATEGORY,
    CALC_PRODUCT_RATING,
    HIGH_VALUE_PROXY_CATEGORY,
    DEFAULT_MOQ,
    MIN_UNIT_PRICE,
    DAYS_SINCE_MERCHANDISABLE,
    TIMESTAMP
FROM
    REGION_BASE_ITEMS B
    LEFT JOIN FEATURES F ON F.ITEM_ID = B.ITEM_ID 
    -- REMOVE DUPLICATED ITEM IDS CAUSED BY PRD - MPV AMBIGUITY, FOR EXAMPLE: PRD-DKCTDQLKH AND PRD-YPIVR9IK7
    QUALIFY ROW_NUMBER() OVER (PARTITION BY B.ITEM_ID ORDER BY IS_LOGOMAKER_ENABLED DESC) = 1
"""


# COMMAND ----------

# DBTITLE 1,v6_users
v6_users_schema = {
    "type": "record",
    "name": "Users",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {
            "name": "USER_ID",
            "type": "string"
        },
        {
            "name": "TOTAL_ORDER_COUNT",
            "type": "int"
        },
        {
            "name": "BOOKING_BUDGET_CATEGORY",
            "type": "int"
        },
        {
            "name": "CURRENT_OPT_STATUS",
            "type": "string",
            "categorical": True
        },
        {
            "name": "IS_DIGITAL_PURCHASER",
            "type": "string",
            "categorical": True
        },
        {
            "name": "LIFECYCLE_SEGMENT",
            "type": "string",
            "categorical": True
        },
        {
            "name": "MASTER_SEGMENT",
            "type": "string",
            "categorical": True
        },
        {
            "name": "INDUSTRY",
            "type": "string",
            "categorical": True
        }
    ]
}

v6_users_query = """
WITH
USERS_COUNTRY AS (
  SELECT
    CUSTOMER_ID,
    COUNTRY,
    COUNT(DISTINCT ORDER_NUMBER) AS COUNTRY_ORDER_NUMBER
  FROM VISTAPRINT.TRANSACTIONS.LOB_BUSINESS_REVIEW
  WHERE ORDER_CREATED_DATE > DATEADD(MONTH, -15, CURRENT_DATE())
  GROUP BY CUSTOMER_ID, COUNTRY
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNTRY_ORDER_NUMBER DESC) = 1
),
ORDERS_INFO AS (
  SELECT
    CUSTOMER_ID,
    COUNT(DISTINCT ORDER_NUMBER) AS TOTAL_ORDER_COUNT,
    SUM(TOTAL_BOOKINGS) AS TOTAL_BOOKINGS
  FROM VISTAPRINT.TRANSACTIONS.LOB_BUSINESS_REVIEW
  WHERE ORDER_CREATED_DATE > DATEADD(MONTH, -15, CURRENT_DATE())
  GROUP BY CUSTOMER_ID
),
LIFECYCLE_INFO AS (
  SELECT
    CUSTOMER_ID,
    LIFECYCLE_SEGMENT_NAME
  FROM VISTAPRINT.SHOPPER.CURRENT_LIFECYCLE_SEGMENT
  WHERE IS_CURRENT = 1
),
USERS_INFO AS (
  SELECT
    CT.CANONICAL_ID AS USER_ID,
    CT.CUSTOMER_ID,
    IFNULL(CT.IS_DIGITAL_PURCHASER, 0) AS IS_DIGITAL_PURCHASER,
    IFNULL(CT.OPT_STATUS, 'Unknown') AS CURRENT_OPT_STATUS,
    IFNULL(MS.MASTER_SEGMENT, 'Unknown') AS MASTER_SEGMENT,
    IFNULL(CV.INDUSTRY_L1, 'Unknown') AS INDUSTRY,
    IFNULL(OI.TOTAL_ORDER_COUNT, 0) AS TOTAL_ORDER_COUNT,
    IFNULL(OI.TOTAL_BOOKINGS, 0) AS TOTAL_BOOKINGS,
    IFNULL(LC.LIFECYCLE_SEGMENT_NAME, 'Unknown') AS LIFECYCLE_SEGMENT,
  FROM VISTAPRINT.SHOPPER.CUSTOMER_TRAITS CT
  LEFT JOIN USERS_COUNTRY UC 
    ON UC.CUSTOMER_ID = CT.CUSTOMER_ID
  LEFT JOIN ORDERS_INFO OI 
    ON OI.CUSTOMER_ID = CT.CUSTOMER_ID
  LEFT JOIN LIFECYCLE_INFO LC 
    ON LC.CUSTOMER_ID = CT.CUSTOMER_ID
  LEFT JOIN VISTAPRINT.SHOPPER.CUSTOMER_INDUSTRY_VERTICAL CV 
    ON CV.CUSTOMER_ID = CT.CUSTOMER_ID
  LEFT JOIN VISTAPRINT.SHOPPER.MASTER_SEGMENT MS 
    ON MS.CUSTOMER_ID = CT.CUSTOMER_ID
  WHERE IFNULL(UC.COUNTRY, CT.CUSTOMER_COUNTRY) IN {locales}
    AND DATE(GREATEST(
      COALESCE(TO_DATE(CT.REGISTRATION_DATETIME), '1900-01-01'),
      COALESCE(TO_DATE(CT.LAST_ORDER_DATE), '1900-01-01'),
      COALESCE(TO_DATE(CT.LAST_PAGE_VIEW_TIMESTAMP), '1900-01-01')
    )) > DATEADD(MONTH, -15, CURRENT_DATE())
    AND CT.IS_VISTA_EMPLOYEE = 0
    AND UPPER(CT.OPT_STATUS) = 'OPTED IN'
    AND CT.CANONICAL_ID IS NOT NULL
),
COMBINED_USER_INFO AS (
  SELECT
    UI.USER_ID,
    UI.CURRENT_OPT_STATUS,
    IFNULL(UI.TOTAL_ORDER_COUNT, 0) AS TOTAL_ORDER_COUNT,
    TO_VARCHAR(UI.IS_DIGITAL_PURCHASER) AS IS_DIGITAL_PURCHASER,
    UI.MASTER_SEGMENT,
    UI.INDUSTRY,
    NTILE(20) OVER (ORDER BY IFNULL(UI.TOTAL_BOOKINGS, 0) DESC) AS SPEND_VENTILE,
    UI.LIFECYCLE_SEGMENT
  FROM USERS_INFO UI
)
SELECT
  USER_ID,
  TOTAL_ORDER_COUNT,
  CASE
    WHEN SPEND_VENTILE = 1 THEN 1
    WHEN SPEND_VENTILE = 2 THEN 2
    WHEN SPEND_VENTILE IN (3, 4) THEN 3
    WHEN SPEND_VENTILE = 5 THEN 4
    WHEN SPEND_VENTILE IN (6, 7, 8) THEN 5
    WHEN SPEND_VENTILE IN (9, 10, 11) THEN 6
    ELSE 7
  END AS BOOKING_BUDGET_CATEGORY,
  CURRENT_OPT_STATUS,
  IS_DIGITAL_PURCHASER,
  LIFECYCLE_SEGMENT,
  MASTER_SEGMENT,
  INDUSTRY
FROM COMBINED_USER_INFO
"""

# COMMAND ----------

# DBTITLE 1,v6_interactions
v6_interactions_schema = {
    "type": "record",
    "name": "Interactions",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {
            "name": "USER_ID",
            "type": "string"
        },
        {
            "name": "ITEM_ID",
            "type": "string"
        },
        {
            "name": "TIMESTAMP",
            "type": "long"
        },
        {
            "name": "PAGE_SECTION",
            "type": "string",
            "categorical": True
        },
        {
            "name": "LOCALE",
            "type": "string",
            "categorical": True
        },
        {
            "name": "EVENT_TYPE",
            "type": "string"
        },
    ]
}

v6_interactions_query = """
WITH 
PRODUCT_VIEWED_EVENTS AS (
    SELECT 
        NVL(PV.USER_ID, UI2.VALUE) AS USER_ID,
        PV.PRODUCT_ID AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        RANK() OVER(PARTITION BY PV.CONTEXT_PAGE_SEARCH ORDER BY PV.TIMESTAMP) AS R,
        ROW_NUMBER() OVER (PARTITION BY PV.VISIT, PV.PRODUCT_ID, PV.PAGE_SECTION ORDER BY PV.TIMESTAMP ) AS ROW_NUM 
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_VIEWED PV
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI
        ON PV.USER_ID IS NULL 
        AND UI.TYPE = 'anonymous_id' 
        AND UI.VALUE = PV.ANONYMOUS_ID
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI2
        ON UI2.TYPE = 'user_id' 
        AND UI.CANONICAL_SEGMENT_ID = UI2.CANONICAL_SEGMENT_ID
    LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS U 
        ON U.ID = PV.USER_ID
    WHERE (
        (  PV.USER_ID IS NOT NULL AND PV.USER_ID NOT LIKE '%@%') 
        OR
        ( UI2.VALUE IS NOT NULL AND UI2.VALUE NOT LIKE '%@%')
    )
    AND PV.PRODUCT_ID NOT LIKE '%PRD-%'
    AND (PV.PAGE_SECTION IN ('Configure - Recommendation', 'Gallery')
    --exclude Studio's product views that already have a product clicked event in recommendation matching components
    OR (PV.PAGE_SECTION = 'Studio' AND PV.CONTEXT_PAGE_URL NOT LIKE '%recommendationsId=%')
    --exclude Product Page viewed events that come from Cross Sell static recommendation components
    OR (PV.PAGE_SECTION = 'Product Page' AND PV.CONTEXT_PAGE_REFERRER NOT LIKE '%/xs/%'))
    --exclude Product Page viewed events that come from Search
    AND PV.CONTEXT_PAGE_REFERRER NOT LIKE '%/search%'
    AND PV.LOCALE IN {locales}
    AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
    AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) =1
), 
PRODUCT_VIEWED_OUTPUT AS (
    SELECT DISTINCT 
        USER_ID, 
        ITEM_ID, 
        TIMESTAMP, 
        PAGE_SECTION, 
        LOCALE,
        CASE 
            WHEN PAGE_SECTION = 'Configure - Recommendation' THEN 'addToCart'
            WHEN PAGE_SECTION IN ('Gallery', 'Studio') THEN 'click'
            ELSE 'productView' 
        END AS EVENT_TYPE
    FROM PRODUCT_VIEWED_EVENTS
    WHERE ( PAGE_SECTION <> 'Configure - Recommendation' OR R=1 ) 
    AND NOT (PAGE_SECTION IN ('Gallery', 'Studio') AND ROW_NUM > 1)
),

PRODUCT_ADDED AS (
    SELECT DISTINCT 
        NVL(PV.USER_ID, UI2.VALUE) AS USER_ID,
        PV.PRODUCT_ID AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        'addToCart' AS EVENT_TYPE
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED PV
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI
        ON PV.USER_ID IS NULL 
        AND UI.TYPE = 'anonymous_id' 
        AND UI.VALUE = PV.ANONYMOUS_ID
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI2
        ON UI2.TYPE = 'user_id' 
        AND UI.CANONICAL_SEGMENT_ID = UI2.CANONICAL_SEGMENT_ID
    LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS U 
        ON U.ID = PV.USER_ID
    WHERE (
        ( PV.USER_ID IS NOT NULL AND PV.USER_ID NOT LIKE '%@%') 
        OR
        ( UI2.VALUE IS NOT NULL AND UI2.VALUE NOT LIKE '%@%')
        )
    AND PV.PRODUCT_ID NOT LIKE '%PRD-%'
    AND PV.ROUTE = 'recommendations'
    AND PV.LOCALE IN {locales}
    AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
    AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
),

PRODUCT_ADDED_TO_WISHLIST AS (
    SELECT 
        DISTINCT NVL(PV.USER_ID, UI2.VALUE) AS USER_ID,
        PV.PRODUCT_ID AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        'addToFavorites' AS EVENT_TYPE
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_ADDED_TO_WISHLIST PV
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI
        ON PV.USER_ID IS NULL 
        AND UI.TYPE = 'anonymous_id' 
        AND UI.VALUE = PV.ANONYMOUS_ID
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI2
        ON UI2.TYPE = 'user_id' 
        AND UI.CANONICAL_SEGMENT_ID = UI2.CANONICAL_SEGMENT_ID
    LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS U 
        ON U.ID = PV.USER_ID
    WHERE (
        ( PV.USER_ID IS NOT NULL AND PV.USER_ID NOT LIKE '%@%') 
        OR
        ( UI2.VALUE IS NOT NULL AND UI2.VALUE NOT LIKE '%@%')
    )
    AND PV.PRODUCT_ID NOT LIKE '%PRD-%'
    AND PV.LOCALE IN {locales}
    AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
    AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1
    AND PAGE_SECTION IN ('Search Results Page', 'Product Page', 'My Account')
),

PRODUCT_CLICKED AS (
    SELECT DISTINCT 
        NVL(PV.USER_ID, UI2.VALUE) AS USER_ID,
        PV.PRODUCT_ID AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        'click' AS EVENT_TYPE
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.PRODUCT_CLICKED PV
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI
        ON PV.USER_ID IS NULL 
        AND UI.TYPE = 'anonymous_id' 
        AND UI.VALUE=PV.ANONYMOUS_ID
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI2
        ON UI2.TYPE = 'user_id' 
        AND UI.CANONICAL_SEGMENT_ID=UI2.CANONICAL_SEGMENT_ID
    LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS U 
        ON U.ID = PV.USER_ID
    WHERE (
            ( PV.USER_ID IS NOT NULL AND PV.USER_ID NOT LIKE '%@%') 
            OR
            ( UI2.VALUE IS NOT NULL AND UI2.VALUE NOT LIKE '%@%')
        )
    AND PV.PRODUCT_ID NOT LIKE '%PRD-%'
    AND PV.LOCALE IN {locales}
    AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
    AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
    AND PV.ROUTE = 'recommendations'
    AND (
            PV.PAGE_SECTION IN ('Design Services', 'Configure - Recommendation', 'My Account')
        OR (
            PV.PAGE_SECTION = 'Cart'
            --include only product clicked events from matching
            AND NVL(LOWER(PV.LIST_SECTION_ID), LOWER(PV.OFFER_TYPE)) LIKE '%matching%'
        )
        OR (
            PV.PAGE_SECTION = 'Home Page'
            --include only product clicked events from matching
            AND NVL(LOWER(PV.LIST_SECTION_ID), LOWER(PV.OFFER_TYPE)) LIKE '%matching%'
        )
    )
),

ORDER_COMPLETED_USERS AS (
    SELECT DISTINCT 
        ID, 
        CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.USERS
),

ORDER_COMPLETED AS (
    SELECT 
        PV.USER_ID,
        F.VALUE:product_id::VARCHAR AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        'purchase' AS EVENT_TYPE
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.ORDER_COMPLETED PV
    LEFT JOIN ORDER_COMPLETED_USERS U 
        ON U.ID = PV.USER_ID,
        LATERAL FLATTEN(INPUT => PARSE_JSON(PV.PRODUCTS)) F
    WHERE (PV.USER_ID IS NOT NULL AND USER_ID NOT LIKE '%@%')
    AND PV.LOCALE IN {locales}
    AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
    AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
    AND F.VALUE:product_id::VARCHAR NOT LIKE 'PRD-%'
),

SEARCH_CLICKED AS (
    SELECT DISTINCT 
        NVL(PV.USER_ID, UI2.VALUE) AS USER_ID,
        PV.PRODUCT_ID AS ITEM_ID,
        DATE_PART(EPOCH_SECOND, PV.TIMESTAMP) AS TIMESTAMP,
        PV.PAGE_SECTION,
        PV.LOCALE,
        'click' AS EVENT_TYPE,
    FROM VISTAPRINT.WEB_TRACKING_EVENTS.SEARCH_RESULT_CLICKED PV
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI
        ON PV.USER_ID IS NULL 
        AND UI.TYPE = 'anonymous_id' 
        AND UI.VALUE=PV.ANONYMOUS_ID
    LEFT JOIN VISTAPRINT.PERSONALIZATION.USER_IDENTIFIERS UI2
        ON UI2.TYPE = 'user_id' 
        AND UI.CANONICAL_SEGMENT_ID=UI2.CANONICAL_SEGMENT_ID
    LEFT JOIN VISTAPRINT.WEB_TRACKING_EVENTS.USERS U 
        ON U.ID = PV.USER_ID
    WHERE (
            ( PV.USER_ID IS NOT NULL AND PV.USER_ID NOT LIKE '%@%') 
            OR
            ( UI2.VALUE IS NOT NULL AND UI2.VALUE NOT LIKE '%@%')
        )
        AND PV.PRODUCT_ID NOT LIKE '%PRD-%'
        AND PV.LOCALE IN {locales}
        AND PV.TIMESTAMP > DATEADD(MONTH, -15, CURRENT_DATE()) AND PV.TIMESTAMP < CURRENT_DATE()
        AND IFNULL(U.CUSTOM_TRACKING_PREFERENCES_FUNCTIONAL,1) = 1 
        and PV.PAGE_SECTION = 'Search Results Page'
),

UNION_CTE AS (
    SELECT {limit_filter} * FROM PRODUCT_VIEWED_OUTPUT
    UNION ALL
    SELECT {limit_filter} * FROM PRODUCT_ADDED
    UNION ALL
    SELECT {limit_filter} * FROM PRODUCT_CLICKED
    UNION ALL
    SELECT {limit_filter} * FROM ORDER_COMPLETED
    UNION ALL
    SELECT {limit_filter} * FROM PRODUCT_ADDED_TO_WISHLIST
    UNION ALL
    SELECT {limit_filter} * FROM SEARCH_CLICKED
)

SELECT 
    A.USER_ID,
    CASE WHEN A.ITEM_ID = 'caBasicTShirts' THEN 'gildanInkPrintedHeavyCottonShortSleeveTShirtNa' ELSE A.ITEM_ID END AS ITEM_ID,
    A.TIMESTAMP,
    A.PAGE_SECTION,
    A.LOCALE,
    A.EVENT_TYPE,
FROM UNION_CTE A
JOIN VISTAPRINT.SHOPPER.CUSTOMER_TRAITS CT
    ON A.USER_ID = CT.CANONICAL_ID
    AND CT.IS_VISTA_EMPLOYEE = 0
    AND CT.IS_ANONYMOUS_USER = 0
"""

v6_interactions_event_types = ["productView", "purchase", "click", "addToCart", "addToFavorites"]

# COMMAND ----------

# DBTITLE 1,definitions
definitions = {
    "v6": {
        "items": {
            "schema": v6_items_schema,
            "query": v6_items_query
        },
        "users": {
            "schema": v6_users_schema,
            "query": v6_users_query
        },
        "interactions": {
            "schema": v6_interactions_schema,
            "query": v6_interactions_query,
            "event_types": v6_interactions_event_types
        }
    }
}
