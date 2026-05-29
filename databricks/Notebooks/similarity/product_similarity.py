# Databricks notebook source
artifact_user = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryUser")
artifact_password = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryPwd")

artifactory_url_virtual = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple"
artifactory_url_local = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-local/simple"

%pip install --index-url {artifactory_url_virtual} vp-dna==2.1.0 vista_dna_akeyless==1.0.8

%pip install colormath==3.0.0
%pip install sentence-transformers==3.0.1
%pip install tensorflow==2.16.2
%pip install tf-keras==2.17.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

if environment == 'dev':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_dev"
    akeyless_access_id = "p-60q2bjh345fy"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'dna_e2_dev_productrecommendations_a0ce4e86_2360_460e_8100_81a39afe3cf0'
    current_embedding_table_name = 'ps_config_embeddings'
    daily_embedding_table_name = 'ps_config_embeddings_daily'

elif environment == 'stg':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_stg"
    akeyless_access_id = "p-oium5k724hbj"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'vista_stg_dna_ppp_product_recomme_e481bcfd_9a77_4e0c_90bb_56f5d3644c20'
    current_embedding_table_name = 'ps_config_embeddings'
    daily_embedding_table_name = 'ps_config_embeddings_daily'

elif environment == 'prd':
    db = "dna"
    output_db = "dna"
    schema = "personalization"
    akeyless_access_id = "p-hbnpveay8mtx"
    substitution_items_dynamo_table_name = "substitution_items_prd"
    unity_schema = 'vista_prd_dna_ppp_product_recomme_a6f65d5f_cc45_4930_95d5_7b4533446ae7'
    current_embedding_table_name = 'ps_config_embeddings'
    daily_embedding_table_name = 'ps_config_embeddings_daily'
    
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
daily_embedding_table_name: {daily_embedding_table_name}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

# MAGIC %run ./similarity_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

def cosine_similarity(vec1, vec2):
    return float(1 - scipy.spatial.distance.cosine(vec1, vec2))
cosine_similarity_udf = udf(cosine_similarity, FloatType())


def get_md5(s):
    ordered_options = dict(sorted(s.items()))
    return hashlib.md5(json.dumps(ordered_options).encode('utf-8')).hexdigest()
md5_udf = udf(get_md5, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Embeddings

# COMMAND ----------

config_embeddings_df = spark.read.format("delta").table(f"vista.{unity_schema}.{current_embedding_table_name}")
config_embeddings_df = config_embeddings_df.repartition(200)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Similarity Comparison

# COMMAND ----------

# MAGIC %run ./similarity_utils

# COMMAND ----------

mu = ps_metrics()

# COMMAND ----------

# Cols to select
base_product_cols = ['INDEX','PRODUCT_KEY','PRODUCT_VERSION','USER_SELECTIONS_CONFIG_ONLY','USER_SELECTIONS_CONFIG_ONLY_SOURCE','MIN_QUANTITY','CONFIG_CONSTRAINT','INACTIVE_ITEM_FLAG','COLOR_NAME']
source_base_product_cols = [col(f"source_df.{attr}").alias(f"SOURCE_{attr}") for attr in base_product_cols if attr != 'USER_SELECTIONS_CONFIG_ONLY']
comparison_base_product_cols = [col(f"comparison_df.{attr}").alias(f"COMPARISON_{attr}") for attr in base_product_cols if attr != 'USER_SELECTIONS_CONFIG_ONLY_SOURCE']

metric_attributes = list(set(val for sublist in mu.relevant_attributes.values() for val in sublist))
source_metric_cols = [col(f"source_df.{attr}").alias(f"SOURCE_{attr}") for attr in metric_attributes]
comparison_metric_cols = [col(f"comparison_df.{attr}").alias(f"COMPARISON_{attr}") for attr in metric_attributes]

# COMMAND ----------

product_group_comparison = {'Outerwear': {'Outerwear'},
 'Hats': {'Beanies','Hats'},
 'T-shirts': {'T-shirts'},
 'Polos': {'Polos'},
 'Sweaters': {'Sweaters'},
 'Sportswear': {'Sportswear'},
 'Dress Shirts': {'Dress Shirts'},
 'Beanies': {'Beanies','Hats'},
 'Bottoms': {'Bottoms'}}

pg_comparison_df = spark.createDataFrame(
    [(k, v) for k, values in product_group_comparison.items() for v in values],
    ["SOURCE_PG", "COMPARISON_PG"]
)

source_df = config_embeddings_df.alias("source_df")
comparison_df = config_embeddings_df.alias("comparison_df")

combined_df = source_df.join(
    comparison_df,
    (col("source_df.MARKET") == col("comparison_df.MARKET")) & 
    (col("source_df.PRODUCT_KEY") != col("comparison_df.PRODUCT_KEY"))
).join(
    pg_comparison_df,
    (col("source_df.PRODUCT_GROUP") == col("SOURCE_PG")) & 
    (col("comparison_df.PRODUCT_GROUP") == col("COMPARISON_PG"))
).repartition(200).cache()

# COMMAND ----------

pairwise_product_df = combined_df.withColumn(
    "PRODUCT_SIMILARITY", 
    cosine_similarity_udf(col("source_df.FULL_EMBEDDING"), col("comparison_df.FULL_EMBEDDING"))
).select(
    col("source_df.MARKET"),
    col("source_df.RECS_MODEL_REGION"),
    col("source_df.PRODUCT_GROUP").alias("SOURCE_PRODUCT_GROUP"),
    *source_base_product_cols,
    *comparison_base_product_cols,
    *source_metric_cols, 
    *comparison_metric_cols ,
    "PRODUCT_SIMILARITY"
).withColumnRenamed("SOURCE_USER_SELECTIONS_CONFIG_ONLY_SOURCE", "SOURCE_USER_SELECTIONS_CONFIG_ONLY")

# COMMAND ----------

pairwise_product_df = pairwise_product_df.where(pairwise_product_df.PRODUCT_SIMILARITY > 0.65)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Prep & Filtering

# COMMAND ----------

color_similarity_udf = udf(mu.color_similarity, FloatType())

color_filter_exclusion = ['Bottoms']
color_name_match_override = ['pink']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filters

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Active product filtering

# COMMAND ----------

pairwise_product_df = pairwise_product_df.where(pairwise_product_df.COMPARISON_INACTIVE_ITEM_FLAG == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contraint filtering

# COMMAND ----------

config_embeddings_df.where(config_embeddings_df.CONFIG_CONSTRAINT == 1).orderBy(['RECS_MODEL_REGION', 'MARKET', 'INDEX'], ascending=[True, True, True]).display()

# COMMAND ----------

# remove constrained items from item lists (comparision), but keep them as source
pairwise_product_df = pairwise_product_df.where(pairwise_product_df.COMPARISON_CONFIG_CONSTRAINT == 0).cache()

# COMMAND ----------

# apply MOQ business rule (comparison MOQ is less than or equal)
unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_MIN_QUANTITY >= pairwise_product_df.COMPARISON_MIN_QUANTITY)

# # calculate standalone color similarity & filter
unranked_sim_df = unranked_sim_df.withColumn("COLOR_SIMILARITY", color_similarity_udf(unranked_sim_df.SOURCE_PRODUCT_GROUP, unranked_sim_df.SOURCE_COLOR, unranked_sim_df.COMPARISON_COLOR))

unranked_sim_df = unranked_sim_df.where(
    ((~unranked_sim_df.SOURCE_PRODUCT_GROUP.isin(color_filter_exclusion)) & 
     (unranked_sim_df.COLOR_SIMILARITY >= 0.83)) | 
    ((unranked_sim_df.SOURCE_PRODUCT_GROUP.isin(color_filter_exclusion)) & 
     (unranked_sim_df.COLOR_SIMILARITY >= 0.5))
)

unranked_sim_df = unranked_sim_df.where(
    (~unranked_sim_df.SOURCE_COLOR_NAME.isin(color_name_match_override)) | 
    ((unranked_sim_df.SOURCE_COLOR_NAME.isin(color_name_match_override)) & 
     (unranked_sim_df.SOURCE_COLOR_NAME == unranked_sim_df.COMPARISON_COLOR_NAME))
)

# restrict one product config per product: color first, then config similarity
comparison_prod_ranking = Window.partitionBy("SOURCE_INDEX","COMPARISON_PRODUCT_KEY").orderBy(desc("COLOR_SIMILARITY"), desc("PRODUCT_SIMILARITY"), "COMPARISON_INDEX")

unranked_sim_df = unranked_sim_df.withColumn("COMPARISON_CONFIG_RANK", rank().over(comparison_prod_ranking))
unranked_sim_df = unranked_sim_df.where(unranked_sim_df.COMPARISON_CONFIG_RANK == 1)


# order product by similarity score - color as tiebreaker
sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("PRODUCT_SIMILARITY"), desc("COLOR_SIMILARITY"), desc("COMPARISON_PRODUCT_KEY")) 

ranked_df = unranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking))

ranked_df = ranked_df.where(ranked_df.FINAL_SIMILARITY_RANK <= 30)

# COMMAND ----------

ranked_df = ranked_df.withColumns({
    "SOURCE_REFERENCE_KEY_NO_SIZE": struct(
                                    col("SOURCE_PRODUCT_KEY").alias("key"),
                                    col("SOURCE_PRODUCT_VERSION").alias("version"), 
                                    col("SOURCE_USER_SELECTIONS_CONFIG_ONLY").alias("options")),
    "COMPARISON_REFERENCE_KEY_NO_SIZE": struct(
                                    col("COMPARISON_PRODUCT_KEY").alias("key"),
                                    col("COMPARISON_PRODUCT_VERSION").alias("version"), 
                                    col("COMPARISON_USER_SELECTIONS_CONFIG_ONLY").alias("options"),
                                    col("PRODUCT_SIMILARITY").alias("similarity_score"),
                                    col("COLOR_SIMILARITY").alias("color_similarity_score"))}).drop("COMPARISON_CONFIG_RANK").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC #### Undefined metric values

# COMMAND ----------

undefined_vals = mu.undefined_values(config_embeddings_df)
if len(undefined_vals) > 0:
    undefined_vals_df = spark.createDataFrame(undefined_vals, ['ATTRIBUTE', 'VALUE'])
    undefined_vals_df.display()
else:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metric evaluation examples

# COMMAND ----------

gender_precision_udf = udf(mu.gender_precision, IntegerType())
type_precision_udf = udf(mu.type_precision, IntegerType())
deco_tech_precision_udf = udf(mu.deco_tech_precision, IntegerType())
deco_location_precision_udf = udf(mu.deco_location_precision, IntegerType())
sleeve_style_precision_udf = udf(mu.sleeve_style_precision, IntegerType())
fit_type_precision_udf = udf(mu.fit_type_precision, IntegerType())

# COMMAND ----------

gender_unique_vals = config_embeddings_df.select(config_embeddings_df.GENDER).distinct().rdd.flatMap(lambda x: x).collect()
gender_unique_df1 = spark.createDataFrame(gender_unique_vals, StringType()).selectExpr("value as GENDER1")
gender_unique_df2 = spark.createDataFrame(gender_unique_vals, StringType()).selectExpr("value as GENDER2")

gender_unique_df = gender_unique_df1.crossJoin(gender_unique_df2)

gender_metric_test = gender_unique_df.withColumn('GENDER_CORRECT', gender_precision_udf(lit('T-shirts'), gender_unique_df.GENDER1, gender_unique_df.GENDER2))
gender_metric_test.display()

# COMMAND ----------

type_unique_vals = config_embeddings_df.select(config_embeddings_df.TYPE).distinct().rdd.flatMap(lambda x: x).collect()
type_unique_df1 = spark.createDataFrame(type_unique_vals, StringType()).selectExpr("value as type1")
type_unique_df2 = spark.createDataFrame(type_unique_vals, StringType()).selectExpr("value as type2")

type_unique_df = type_unique_df1.crossJoin(type_unique_df2)

type_metric_test = type_unique_df.withColumn('TYPE_CORRECT', type_precision_udf(lit('Outerwear'), type_unique_df.type1, type_unique_df.type2))
type_metric_test.display()

# COMMAND ----------

deco_tech_unique_vals = config_embeddings_df.select(config_embeddings_df.DECORATION_TECHNOLOGY_1).distinct().rdd.flatMap(lambda x: x).collect()
deco_tech_unique_df1 = spark.createDataFrame(deco_tech_unique_vals, StringType()).selectExpr("value as deco_tech1")
deco_tech_unique_df2 = spark.createDataFrame(deco_tech_unique_vals, StringType()).selectExpr("value as deco_tech2")

deco_tech_unique_df = deco_tech_unique_df1.crossJoin(deco_tech_unique_df2)

deco_tech_metric_test = deco_tech_unique_df.withColumn('DECO_TECH_CORRECT', deco_tech_precision_udf(lit('Outerwear'), deco_tech_unique_df.deco_tech1, deco_tech_unique_df.deco_tech2))
deco_tech_metric_test.display()

# COMMAND ----------

deco_location_unique_vals = config_embeddings_df.select(config_embeddings_df.DECORATION_LOCATION_1).distinct().rdd.flatMap(lambda x: x).collect()
deco_location_unique_df1 = spark.createDataFrame(deco_location_unique_vals, StringType()).selectExpr("value as deco_location1")
deco_location_unique_df2 = spark.createDataFrame(deco_location_unique_vals, StringType()).selectExpr("value as deco_location2")

deco_location_unique_df = deco_location_unique_df1.crossJoin(deco_location_unique_df2)

deco_location_metric_test = deco_location_unique_df.withColumn('DECO_LOCATION_CORRECT', deco_location_precision_udf(lit('Outerwear'), deco_location_unique_df.deco_location1, deco_location_unique_df.deco_location2))
deco_location_metric_test.display()

# COMMAND ----------

sleeve_style_unique_vals = config_embeddings_df.select(config_embeddings_df.SLEEVE_STYLE).distinct().rdd.flatMap(lambda x: x).collect()
sleeve_style_unique_df1 = spark.createDataFrame(sleeve_style_unique_vals, StringType()).selectExpr("value as sleeve_style1")
sleeve_style_unique_df2 = spark.createDataFrame(sleeve_style_unique_vals, StringType()).selectExpr("value as sleeve_style2")

sleeve_style_unique_df = sleeve_style_unique_df1.crossJoin(sleeve_style_unique_df2)

sleeve_style_metric_test = sleeve_style_unique_df.withColumn('SLEEVE_STYLE_CORRECT', sleeve_style_precision_udf(lit('T-shirts'), sleeve_style_unique_df.sleeve_style1, sleeve_style_unique_df.sleeve_style2))
sleeve_style_metric_test.display()

# COMMAND ----------

fit_type_unique_vals = config_embeddings_df.select(config_embeddings_df.FIT_TYPE).distinct().rdd.flatMap(lambda x: x).collect()
fit_type_unique_df1 = spark.createDataFrame(fit_type_unique_vals, StringType()).selectExpr("value as fit_type1")
fit_type_unique_df2 = spark.createDataFrame(fit_type_unique_vals, StringType()).selectExpr("value as fit_type2")

fit_type_unique_df = fit_type_unique_df1.crossJoin(fit_type_unique_df2)

fit_type_metric_test = fit_type_unique_df.withColumn('FIT_TYPE_CORRECT', fit_type_precision_udf(lit('Hats'), fit_type_unique_df.fit_type1, fit_type_unique_df.fit_type2))
fit_type_metric_test.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metric calculation

# COMMAND ----------

metric_df = ranked_df.withColumns({
    'GENDER_EVAL': gender_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                        ranked_df.SOURCE_GENDER, 
                                      ranked_df.COMPARISON_GENDER),
    'COLOR_EVAL': color_similarity_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                                ranked_df.SOURCE_COLOR, 
                                               ranked_df.COMPARISON_COLOR),
    'TYPE_EVAL': type_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                    ranked_df.SOURCE_TYPE, 
                                   ranked_df.COMPARISON_TYPE),
    'DECORATION_TECHNOLOGY_1_EVAL': deco_tech_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                            ranked_df.SOURCE_DECORATION_TECHNOLOGY_1, 
                                             ranked_df.COMPARISON_DECORATION_TECHNOLOGY_1),
    'DECORATION_LOCATION_1_EVAL': deco_location_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                                    ranked_df.SOURCE_DECORATION_LOCATION_1, 
                                                     ranked_df.COMPARISON_DECORATION_LOCATION_1),
    'SLEEVE_STYLE_EVAL': sleeve_style_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                                    ranked_df.SOURCE_SLEEVE_STYLE, 
                                                   ranked_df.COMPARISON_SLEEVE_STYLE),
    'FIT_TYPE_EVAL': fit_type_precision_udf(ranked_df.SOURCE_PRODUCT_GROUP,
                                                    ranked_df.SOURCE_FIT_TYPE, 
                                                   ranked_df.COMPARISON_FIT_TYPE)
}).cache()

# COMMAND ----------

rank_thresholds = [5, 10, 15]
rank_threshold_conditions = [when(col("FINAL_SIMILARITY_RANK") <= rt, 1).otherwise(0).alias(f"rank_{rt}") for rt in rank_thresholds]

metric_df_with_ranks = metric_df.select("*", *rank_threshold_conditions).cache()

# COMMAND ----------

agg_exprs = [
    avg(when(col(f"rank_{rt}") == 1, col(f"{metric_name}_EVAL")).otherwise(None)).alias(f"{metric_name}_at_{rt}")
    for metric_name in metric_attributes
    for rt in rank_thresholds
]

# product group metrics
pg_grouped_df = metric_df_with_ranks.groupBy("MARKET","RECS_MODEL_REGION","SOURCE_PRODUCT_GROUP").agg(*agg_exprs)

pg_agg_df = pg_grouped_df.select("MARKET","RECS_MODEL_REGION","SOURCE_PRODUCT_GROUP", explode(
    array(
        *[
            struct(lit(rt).alias("METRIC_AT"), lit(metric_name).alias("METRIC_NAME"), col(f"{metric_name}_at_{rt}").alias("METRIC_SCORE"))
            for metric_name in metric_attributes
            for rt in rank_thresholds
        ]
    )
).alias("METRICS")).select("MARKET","RECS_MODEL_REGION","SOURCE_PRODUCT_GROUP", "METRICS.*")

# overall metrics
overall_df = metric_df_with_ranks.groupBy("MARKET","RECS_MODEL_REGION").agg(*agg_exprs)

overall_agg_df = overall_df.select("MARKET","RECS_MODEL_REGION",lit("Overall").alias("SOURCE_PRODUCT_GROUP"), explode(
    array(
        *[
            struct(lit(rt).alias("METRIC_AT"), lit(metric_name).alias("METRIC_NAME"), col(f"{metric_name}_at_{rt}").alias("METRIC_SCORE"))
            for metric_name in metric_attributes
            for rt in rank_thresholds
        ]
    )
).alias("METRICS")).select("MARKET","RECS_MODEL_REGION","SOURCE_PRODUCT_GROUP", "METRICS.*")

agg_metrics = pg_agg_df.union(overall_agg_df)

agg_metrics = agg_metrics.where(agg_metrics.METRIC_SCORE.isNotNull())
agg_metrics = agg_metrics.select("MARKET","RECS_MODEL_REGION","SOURCE_PRODUCT_GROUP", "METRIC_AT", "METRIC_NAME", "METRIC_SCORE").repartition(200).cache()

# COMMAND ----------

metric_write_out = agg_metrics.withColumn('CREATED_AT', current_date())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metrics to snowflake

# COMMAND ----------

# snowflake.execute_nonquery(f"""
# DROP TABLE {output_db}.{schema}.product_similarity_metrics 
# """)

# COMMAND ----------

snowflake.execute_nonquery(f"""
CREATE TABLE IF NOT EXISTS {output_db}.{schema}.product_similarity_metrics (
    RECS_MODEL_REGION                       VARCHAR(16777216),
    MARKET                                  VARCHAR(16777216),
    SOURCE_PRODUCT_GROUP                    VARCHAR(16777216),
    METRIC_AT                               NUMBER(3,0),
    METRIC_NAME                             VARCHAR(16777216),
    METRIC_SCORE                            NUMBER(10,5),
    CREATED_AT                              DATE
    )
CLUSTER BY (MARKET)
""")

# COMMAND ----------

snowflake.table_from_df(
    df=metric_write_out,
    database=db,
    schema=schema,
    table=f"product_similarity_metrics_temp",
    mode="overwrite"
)

# COMMAND ----------

snowflake.execute_nonquery(f"""
MERGE INTO {db}.{schema}.product_similarity_metrics as target
USING {db}.{schema}.product_similarity_metrics_temp as source
ON source.recs_model_region = target.recs_model_region
AND source.market = target.market
AND source.source_product_group = target.source_product_group
AND source.metric_name = target.metric_name
AND source.metric_at = target.metric_at
AND source.created_at = target.created_at
WHEN MATCHED THEN UPDATE SET
    target.metric_score = source.metric_score
WHEN NOT MATCHED THEN 
    INSERT (recs_model_region, market, source_product_group, metric_name, metric_at, created_at, metric_score)
    VALUES (source.recs_model_region, source.market, source.source_product_group, source.metric_name, source.metric_at, source.created_at, source.metric_score)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #Write rankings to snowflake

# COMMAND ----------

snowflake_select_exprs = [c for c in ranked_df.columns]

snowflake_cols_to_drop = ['SOURCE_REFERENCE_KEY_NO_SIZE','COMPARISON_REFERENCE_KEY_NO_SIZE']

snowflake_write_out = ranked_df.withColumn("CREATED_AT", current_timestamp()).select(*snowflake_select_exprs
                                       ,md5_udf(col("SOURCE_REFERENCE_KEY_NO_SIZE.options")).alias("HASHED_SOURCE_OPTIONS")
                                       ,md5_udf(col("COMPARISON_REFERENCE_KEY_NO_SIZE.options")).alias("HASHED_COMPARISON_OPTIONS")
                                       ,"CREATED_AT").drop(*snowflake_cols_to_drop).cache()

# COMMAND ----------

snowflake.table_from_df(
    df=snowflake_write_out,
    database=db,
    schema=schema,
    table=f"product_similarity_universe",
    mode="overwrite")

# COMMAND ----------

# unity 
snowflake_write_out \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"vista.{unity_schema}.product_similarity_universe")

# COMMAND ----------

# MAGIC %md
# MAGIC #Write to DynamoDB

# COMMAND ----------

temp_creds = utils.get_temp_aws_credentials(duration=3600*4)
session = utils.get_temp_session(temp_creds)
dynamo = session.resource('dynamodb', region_name="eu-west-1")
substitution_items_dynamo_table = dynamo.Table(substitution_items_dynamo_table_name)

# COMMAND ----------

# preapre data for writing out
dynamo_new_items_df = ranked_df \
    .withColumn("hashed_source_options", md5_udf("SOURCE_REFERENCE_KEY_NO_SIZE.options")) \
    .selectExpr(
        "upper(market || '-' || source_product_key) as locale_item",
        "hashed_source_options",
        "COMPARISON_REFERENCE_KEY_NO_SIZE") \
    .groupBy("locale_item", "hashed_source_options") \
    .agg(collect_list('COMPARISON_REFERENCE_KEY_NO_SIZE').alias("products")) \
    .cache()

# COMMAND ----------

# get current items from dynamo
scan_response = substitution_items_dynamo_table.scan()
all_current_dynamo_items = scan_response['Items']
while scan_response.get("LastEvaluatedKey"):
    scan_response = substitution_items_dynamo_table.scan(ExclusiveStartKey=scan_response['LastEvaluatedKey'])
    all_current_dynamo_items.extend(scan_response['Items'])

# COMMAND ----------

# get items to delete
current_dynamo_items_schema = StructType([
    StructField("locale_item", StringType(), True),
    StructField("hashed_source_options", StringType(), True)
])

current_dynamo_items_df = sc.parallelize(all_current_dynamo_items) \
    .map(lambda x: Row(locale_item=x['locale_item'], hashed_source_options=x['hashed_source_options'])) \
    .toDF(current_dynamo_items_schema) \
    .selectExpr("locale_item", "hashed_source_options") \
    .cache()

dynamo_new_keys_df = dynamo_new_items_df \
    .toDF(*[c.lower() for c in dynamo_new_items_df.columns]) \
    .selectExpr("locale_item", "hashed_source_options") \
    .cache()

items_to_delete = current_dynamo_items_df \
    .join(dynamo_new_keys_df, ["locale_item", "hashed_source_options"], "left_anti") \
    .select("locale_item", "hashed_source_options") \
    .cache()

print(f"""
Current items: {current_dynamo_items_df.count()}
New items: {dynamo_new_items_df.count()}
To delete: {items_to_delete.count()}
""")

# COMMAND ----------

# delete items

with substitution_items_dynamo_table.batch_writer() as batch:
    for row in items_to_delete.collect():
        batch.delete_item(Key={
            'locale_item': row["locale_item"],
            'hashed_source_options': row["hashed_source_options"]
        })

# COMMAND ----------

# insert newly generated items

with substitution_items_dynamo_table.batch_writer() as batch:
    for i in dynamo_new_items_df.collect():
        batch.put_item(Item={
            'locale_item': i["locale_item"],
            'hashed_source_options': i["hashed_source_options"],
            'items': [
                {
                    "itemId": p["key"],
                    "version": p["version"],
                    "similarity_score": __builtins__.round(Decimal(p["similarity_score"]), 5),
                    "color_similarity_score": __builtins__.round(Decimal(p["color_similarity_score"]), 5),
                    "options": p["options"]
                }
                for p in i["products"]
            ]
        })

# COMMAND ----------

combined_df.unpersist()
pairwise_product_df.unpersist()
ranked_df.unpersist()
metric_df.unpersist()
metric_df_with_ranks.unpersist()
agg_metrics.unpersist()
snowflake_write_out.unpersist()
dynamo_new_items_df.unpersist()
current_dynamo_items_df.unpersist()
dynamo_new_keys_df.unpersist()
items_to_delete.unpersist()
