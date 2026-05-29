# Databricks notebook source
class Query:
    def __init__(self, sql_parameters):
        self.product_configs = """
                WITH raw_attributes as (
                select  mb.mcp_sku, mb.sku_version, mb.property_name, mb.property_value
                from  VISTAPRINT.PRODUCT.MCP_SKU_PRODUCT_CONFIG m
                join vistaprint.product.product_categorization pc on m.product_key = pc.product_key
                join dna.product.mcp_sku_config_base mb
                on m.mcp_sku = mb.mcp_sku
                and m.sku_version = mb.sku_version
                WHERE is_current = TRUE
                and pc.product_group in ('Beanies','Bottoms','Dress Shirts','Hats','Outerwear','Polos','Sportswear','Sweaters','T-shirts') -- 'Clothing - Other'
                ),

                flattened_attribs as (select *
                from raw_attributes
                pivot(min(PROPERTY_VALUE) for property_name in ('FIT','FIT_FEATURES','POCKETS','FRONT_POCKET_CLOSURES','POCKET','SLEEVES','SPECIAL_FEATURES','WAIST_TYPE','BACK_POCKET_S','FRONT_POCKET_CLOSURE','CHEST_POCKET_TYPE','CUFF_TYPE','CHEST_POCKET_S','QUALITY_ATTRIBUTES','FEATURES','PANELS','BRIM','EYELETS', 'CROWN','CLOSURE','FIT_TYPE','FASTNER_TYPE','DECORATION_LOCATION_1_NAME_PROPERTY', 'DECOATION_LOCATION_1_NAME_PROPERTY','DECORATION_LOCATION1_NAME','DECO_AREA_NAME','AREA_1_LOCATION','DECO_LOCATION_1','DECORATION_LOCATION','DECORATIONLOCATION','MSW_DECORATION_TECHNOLOGY','DECORATION_TECHNOLOGY','DECORATION_LOCATION_1_PROCESS_TYPE_PROPERTY','DECO_1_PROCESS_TYPE','DECORATION_LOCATION_1_PROCESS_TYPE','ZIPPER_PULLS')) as p (MCP_SKU, SKU_VERSION, FIT,FIT_FEATURES,POCKETS,FRONT_POCKET_CLOSURES,POCKET,SLEEVES,SPECIAL_FEATURES,WAIST_TYPE,BACK_POCKET_S,FRONT_POCKET_CLOSURE,CHEST_POCKET_TYPE,CUFF_TYPE,CHEST_POCKET_S,QUALITY_ATTRIBUTES,FEATURES,PANELS,BRIM,EYELETS,CROWN,CLOSURE,FIT_TYPE,FASTNER_TYPE,DECORATION_LOCATION_1_NAME_PROPERTY,DECOATION_LOCATION_1_NAME_PROPERTY,DECORATION_LOCATION1_NAME,DECO_AREA_NAME,AREA_1_LOCATION,DECO_LOCATION_1,DECORATION_LOCATION,DECORATIONLOCATION,MSW_DECORATION_TECHNOLOGY,DECORATION_TECHNOLOGY,DECORATION_LOCATION_1_PROCESS_TYPE_PROPERTY,DECO_1_PROCESS_TYPE,DECORATION_LOCATION_1_PROCESS_TYPE,ZIPPER_PULLS)),

                standard_product_configs AS (
                    SELECT
                        m.PRODUCT_KEY,
                        PRODUCT_VERSION,
                        TRIM(coalesce(GET(COLLAR_TYPE, 0),GET(collar, 0),GET(collar_type_polo, 0)), '"') AS COLLAR_TYPE,
                        TRIM(coalesce(GET(collar_fastener, 0),GET(collar_fastner, 0)), '"') AS COLLAR_FASTENER,
                        TRIM(coalesce(GET(DECORATION_LOCATION_1, 0),GET(msw_decoration_location_1, 0),GET(decoration_location_1_name, 0),GET(printing_process_decoration_location_1, 0), fa.DECORATION_LOCATION_1_NAME_PROPERTY, fa.DECOATION_LOCATION_1_NAME_PROPERTY, fa.DECORATION_LOCATION1_NAME, fa.DECO_AREA_NAME, fa.AREA_1_LOCATION, fa.DECO_LOCATION_1, fa.DECORATION_LOCATION, DECORATIONLOCATION), '"') AS DECORATION_LOCATION_1,
                        --GET(DECORATION_LOCATION_2, 0) AS DECORATION_LOCATION_2,
                        TRIM(coalesce(GET(DECORATION_TECHNOLOGY_1, 0), fa.MSW_DECORATION_TECHNOLOGY, fa.DECORATION_TECHNOLOGY, fa.DECORATION_LOCATION_1_PROCESS_TYPE_PROPERTY, fa.DECO_1_PROCESS_TYPE, fa.DECORATION_LOCATION_1_PROCESS_TYPE), '"') AS DECORATION_TECHNOLOGY_1,
                        TRIM(GET(GENDER, 0), '"') AS GENDER,
                        TRIM(coalesce(GET(MATERIAL, 0),GET(material_details, 0)), '"') AS MATERIAL,
                        --MIN_QUANTITY,
                        NVL(TRIM(GET(m.SLEEVE_STYLE, 0), '"'), fa.SLEEVES) AS SLEEVE_STYLES,
                        coalesce(fa.FIT, fa.FIT_FEATURES) as FIT,
                        coalesce(fa.POCKETS, fa.POCKET) as POCKETS,
                        case when lower(fa.features) <> 'none' then fa.SPECIAL_FEATURES || ', ' || fa.features else fa.SPECIAL_FEATURES end as SPECIAL_FEATURES,
                        fa.FRONT_POCKET_CLOSURES,
                        TRIM(GET(m.type, 0), '"') AS TYPE ,
                        coalesce(TRIM(GET(m.quality, 0), '"'), fa.QUALITY_ATTRIBUTES)  AS quality ,
                        fa.WAIST_TYPE,
                        fa.BACK_POCKET_S,
                        fa.FRONT_POCKET_CLOSURE,
                        fa.CHEST_POCKET_TYPE,
                        fa.CHEST_POCKET_S,
                        fa.CUFF_TYPE,
                        fa.PANELS,
                        fa.BRIM,
                        fa.EYELETS,
                        fa.CROWN,
                        fa.CLOSURE,
                        fa.FIT_TYPE,
                        fa.FASTNER_TYPE,
                        fa.ZIPPER_PULLS
                        
                    FROM
                        VISTAPRINT.PRODUCT.MCP_SKU_PRODUCT_CONFIG m
                        join vistaprint.product.product_categorization pc on m.product_key = pc.product_key
                        left join flattened_attribs fa
                        ON m.MCP_SKU = fa.MCP_SKU AND m.SKU_VERSION = fa.SKU_VERSION
                    WHERE
                        is_current = TRUE
                        and pc.product_group in ('Beanies','Bottoms','Dress Shirts','Hats','Outerwear','Polos','Sportswear','Sweaters','T-shirts') -- 'Clothing - Other'
                    ),
                flattened_pricing as (
                    select
                        MERCHANTPRODUCTID as product_key,
                        merchantproductversion as product_version,
                        JSON :userSelections as user_selections,
                        array_min(JSON :quantities) as quantities_min,
                        prices.VALUE :market :: string as Market,
                        crm.recs_model_region,
                        prices.VALUE :currency :: string as Currency,
                        prices.VALUE :prices as list_prices,
                        JSON :updatedAt :: timestamp_tz as RECORD_UDPATE_STAMP
                    from
                        mcp.pricing.price_enumerations_all as e
                        join standard_product_configs spc on e.merchantproductid = spc.product_key
                        and e.merchantproductversion = spc.product_version,
                        LATERAL FLATTEN(OUTER => TRUE, INPUT => e."JSON" :"listPrices") prices
                        join dna.personalization.recs_country_region_mapping crm
                        on market = crm.country
                    where
                        prices.VALUE :merchantId = 'VISTAPRINT'
                        and crm.recs_model_region = '{recs_region}'
                    qualify row_number() over(partition by MerchantProductId, Market, user_selections order by RECORD_UDPATE_STAMP desc) = 1
                ),
                priced_quantities as (
                    SELECT
                        product_key,
                        user_selections,
                        Market,
                        recs_model_region,
                        Currency,
                        MIN(TRY_TO_NUMBER(f.key)) AS priced_quantities_moq
                    FROM
                        flattened_pricing,
                        LATERAL FLATTEN(input => PARSE_JSON(list_prices)) AS f
                    group by all
                ),
                extracted_moq as (
                    select
                        fd.product_key,
                        fd.product_version,
                        fd.market,
                        fd.recs_model_region,
                        fd.currency,
                        fd.user_selections,
                        case
                            when priced_quantities_moq > quantities_min then priced_quantities_moq
                            else quantities_min
                        end as min_quantity,
                        fd.list_prices,
                        PARSE_JSON(LOWER(GET(fd.list_prices, TO_CHAR(priced_quantities_moq)))) :unitlistprice :untaxed AS local_moq_price,
                        fd.RECORD_UDPATE_STAMP
                    from
                        flattened_pricing fd
                        join priced_quantities pq on fd.product_key = pq.product_key
                        and fd.user_selections = pq.user_selections
                        and fd.market = pq.market
                        and fd.currency = pq.currency
                ),
                priced_configs AS (
                    SELECT
                        spc.product_key,
                        spc.product_version,
                        em.market,
                        em.recs_model_region,
                        spc.collar_type,
                        spc.collar_fastener,
                        spc.material,
                        spc.sleeve_styles as sleeve_style,
                        spc.fit,
                        spc.pockets,
                        spc.special_features,
                        spc.front_pocket_closures,
                        spc.type,
                        spc.quality,
                        spc.waist_type ,
                        spc.back_pocket_s,
                        spc.front_pocket_closure,
                        spc.chest_pocket_type,
                        spc.chest_pocket_s,
                        spc.cuff_type,
                        spc.panels,
                        spc.brim,
                        spc.eyelets,
                        spc.crown,
                        spc.closure,
                        spc.fit_type,
                        spc.fastner_type,
                        spc.zipper_pulls,
                        em.currency,
                        em.min_quantity,
                        coalesce(
                            TRIM(em.user_selections: "Gender", '"'),
                            TRIM(em.user_selections: "gender", '"'),
                            spc.gender
                        ) as gender,
                        coalesce(
                            TRIM(em.user_selections: "Deco Area", '"'),
                            TRIM(em.user_selections: "Decoration Area", '"'),
                            TRIM(em.user_selections: "deco area", '"'),
                            spc.decoration_location_1
                        ) as decoration_location_1,
                        coalesce(
                            TRIM(em.user_selections: "Decoration", '"'),
                            TRIM(em.user_selections: "Decoration Technology", '"'),
                            TRIM(em.user_selections: "Deco Tech", '"'),
                            TRIM(em.user_selections: "decoration technology", '"'),
                            TRIM(em.user_selections: "deco tech", '"'),
                            spc.DECORATION_TECHNOLOGY_1
                        ) as decoration_technology_1,
                        left(coalesce(
                            TRIM(em.user_selections: "Substrate Color", '"'),
                            TRIM(em.user_selections: "substrate color", '"')
                        ),7) AS COLOR,
                        coalesce(
                            TRIM(em.user_selections: "Substrate Color", '"'),
                            TRIM(em.user_selections: "substrate color", '"')
                        ) AS EXACT_COLOR,
                        coalesce(
                            TRIM(em.user_selections: Size, '"'),
                            TRIM(em.user_selections: "size", '"')
                        ) AS size,
                        coalesce(
                            TRIM(em.user_selections: Backside, '"'),
                            TRIM(em.user_selections: "backside", '"')
                        ) AS backside,
                        em.local_moq_price * br.rate AS moq_unit_price,
                        em.RECORD_UDPATE_STAMP,
                        em.user_selections
                    FROM
                        standard_product_configs spc
                        join extracted_moq em on spc.product_key = em.product_key
                        and spc.product_version = em.product_version
                        JOIN VISTAPRINT.TRANSACTIONS.DIM_BUDGET_RATES br ON em.currency = br.currency_from
                        AND br.DATE = DATE (DATEADD(day, - 1, CURRENT_DATE))
                        AND br.currency_to = 'USD'
                )
                SELECT
                    DISTINCT pc.product_key,
                    p.product_version,
                    p.market,
                    p.recs_model_region,
                    pc.product_name,
                    pc.category,
                    pc.subcategory,
                    pc.product_group,
                    p.COLLAR_TYPE,
                    p.COLLAR_FASTENER,
                    p.DECORATION_LOCATION_1,
                    p.Backside,
                    p.DECORATION_TECHNOLOGY_1,
                    p.GENDER,
                    p.MATERIAL,
                    p.MIN_QUANTITY,
                    p.SLEEVE_STYLE,
                        p.fit,
                        p.pockets,
                        p.special_features,
                        p.front_pocket_closures,
                        p.type,
                        p.quality,
                        p.waist_type ,
                        p.back_pocket_s,
                        p.front_pocket_closure,
                        p.chest_pocket_type,
                        p.chest_pocket_s,
                        p.cuff_type,
                        p.panels,
                        p.brim,
                        p.eyelets,
                        p.crown,
                        p.closure,
                        p.fit_type,
                        p.fastner_type,
                        p.zipper_pulls,
                    p.Size,
                    upper(p.COLOR) as COLOR,
                    upper(p.EXACT_COLOR) as EXACT_COLOR,
                    p.moq_unit_price,
                    p.user_selections,
                    case when bi.product_key is null then 1 else 0 end as inactive_item_flag 
                FROM
                    priced_configs p
                    join VISTAPRINT.PRODUCT.PRODUCT_CATEGORIZATION pc ON p.product_key = pc.product_key
                    left join dna.personalization.recommendations_base_items bi
                    on p.product_key = bi.product_key
                    and p.market = bi.country
                where len(p.COLOR) = 7
                    and left(p.COLOR,1) = '#'
                    and regexp_like(right(p.COLOR,6), '^[0-9A-Fa-f]+$')
                    {limit_count}
            """.format(**sql_parameters)
        self.constraints_query = f"""
                select distinct
                    PRODUCT_KEY as source_product_key,
                    PRODUCT_VERSION as source_product_version,
                    COUNTRY as source_market,
                    constraint_attributes AS source_color
                from {db}.{schema}.puns_constraint_items
            """
