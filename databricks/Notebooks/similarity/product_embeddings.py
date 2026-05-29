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

dbutils.widgets.text("recs_region", "")
recs_region = dbutils.widgets.get("recs_region").lower()

if not recs_region:
    raise ValueError("Region is not defined")

# COMMAND ----------

if environment == 'dev':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_dev"
    akeyless_access_id = "p-60q2bjh345fy"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'dna_e2_dev_productrecommendations_a0ce4e86_2360_460e_8100_81a39afe3cf0'
    current_embedding_table_name = 'ps_config_embeddings'
    region_daily_embedding_table_name = f"ps_config_embeddings_daily_{recs_region}"
    region_new_embeddings_table = f"ps_config_embeddings_new_{recs_region}"

elif environment == 'stg':
    db = "sandbox"
    output_db = "sandbox"
    schema = "dna_personalization_stg"
    akeyless_access_id = "p-oium5k724hbj"
    substitution_items_dynamo_table_name = "substitution_items_dev"
    unity_schema = 'vista_stg_dna_ppp_product_recomme_e481bcfd_9a77_4e0c_90bb_56f5d3644c20'
    current_embedding_table_name = 'ps_config_embeddings'
    region_daily_embedding_table_name = f"ps_config_embeddings_daily_{recs_region}"
    region_new_embeddings_table = f"ps_config_embeddings_new_{recs_region}"

elif environment == 'prd':
    db = "dna"
    output_db = "dna"
    schema = "personalization"
    akeyless_access_id = "p-hbnpveay8mtx"
    substitution_items_dynamo_table_name = "substitution_items_prd"
    unity_schema = 'vista_prd_dna_ppp_product_recomme_a6f65d5f_cc45_4930_95d5_7b4533446ae7'
    current_embedding_table_name = 'ps_config_embeddings'
    region_daily_embedding_table_name = f"ps_config_embeddings_daily_{recs_region}"
    region_new_embeddings_table = f"ps_config_embeddings_new_{recs_region}"
    
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
region_daily_embedding_table_name: {region_daily_embedding_table_name}
region_new_embedding_table_name: {region_new_embeddings_table}
region: {recs_region}
""")

# COMMAND ----------

dbutils.widgets.text("unity_schema_widget", f"{unity_schema}")
unity_schema_widget = dbutils.widgets.get("unity_schema_widget")


dbutils.widgets.text("region_daily_embedding_table_name_widget", f"{region_daily_embedding_table_name}")
region_daily_embedding_table_name_widget = dbutils.widgets.get("region_daily_embedding_table_name_widget")


dbutils.widgets.text("region_new_embedding_table_name_widget", f"{region_new_embeddings_table}")
region_new_embedding_table_name_widget = dbutils.widgets.get("region_new_embedding_table_name_widget")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

# MAGIC %run ./query

# COMMAND ----------

# MAGIC %run ./similarity_utils

# COMMAND ----------

import pkg_resources
print(pkg_resources.get_distribution("sentence-transformers").version)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

def hex_to_lab(hex_color):
    rgb_color = sRGBColor.new_from_rgb_hex(hex_color)
    lab_color = convert_color(rgb_color, LabColor).get_value_tuple()
    return list(lab_color)
hex_to_lab_udf = udf(hex_to_lab, ArrayType(FloatType()))

# base simple color list hex
basic_colors_hex = [
# === BLUE ===
    ("blue", "#0000FF"), ("blue", "#000080"), ("blue", "#4682B4"),
    ("blue", "#5F9EA0"), ("blue", "#1E3F66"), ("blue", "#87CEEB"),
    # === PURPLE ===
    ("purple", "#800080"), ("purple", "#9370DB"), ("purple", "#BA55D3"),
    # === RED ===
    ("red", "#FF0000"), ("red", "#DC143C"), ("red", "#B22222"),
    ("red", "#FF6347"), ("red", "#C41E3A"),
    # === BROWN ===
    ("brown", "#A52A2A"), ("brown", "#8B4513"), ("brown", "#D2691E"),
    ("brown", "#654321"), ("brown", "#7B3F00"),
    # === GRAY ===
    ("gray", "#808080"), ("gray", "#A9A9A9"), ("gray", "#D3D3D3"),
    # === PINK ===
    ("pink", "#FFC0CB"), ("pink", "#FFB6C1"), ("pink", "#FADADD"),
    ("pink", "#F5E6E8"), ("pink", "#FFE4E1"),
    # === GREEN ===
    ("green", "#008000"), ("green", "#00FF00"), ("green", "#006400"),
    ("green", "#9ACD32"), ("green", "#98FB98"),
    # === ORANGE ===
    ("orange", "#FFA500"), ("orange", "#FF8C00"), ("orange", "#FFB347"),
    # === YELLOW ===
    ("yellow", "#FFFF00"), ("yellow", "#FFD700"), ("yellow", "#FFFACD"),
    # === WHITE ===
    ("white", "#FFFFFF"), ("white", "#F5F5F5"),
    # === BLACK ===
    ("black", "#000000"),
]

basic_colors_lab = [
    (name, hex_to_lab(hex_code))
    for name, hex_code in basic_colors_hex
]

def closest_color_name(hex_color):
    import numpy
    def patch_asscalar(a):
        return a.item()
    setattr(numpy, "asscalar", patch_asscalar)
    
    c1_rgb = sRGBColor.new_from_rgb_hex(hex_color)
    c1_lab = convert_color(c1_rgb, LabColor)

    closest_color_name = min(
        basic_colors_lab,
        key=lambda item: delta_e_cmc(c1_lab, LabColor(item[1][0], item[1][1], item[1][2]))
    )
    return closest_color_name[0]

closest_color_name_udf = udf(closest_color_name, StringType())

def null_counts(col_name):
    return (
        f.sum(when(col(col_name).isNull() | (col(col_name) == ''), 1).otherwise(0)) 
    ).alias(col_name)

def embed_text(text):
    return model.encode([text])[0].tolist()
embed_udf = udf(embed_text, ArrayType(FloatType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw Data

# COMMAND ----------

 #limit 1000
 sql_parameters = {
            "recs_region": f"{recs_region}"
            ,"limit_count": "" 
        }

queries = Query(sql_parameters)

# COMMAND ----------

product_options_schema = StructType([
    StructField("key", StringType(), False),
    StructField("version", IntegerType(), False),
    StructField("options", MapType(StringType(), StringType()), False)
])

selections_schema = MapType(StringType(), StringType())

def uppercase_color_option(options_map):
    if options_map is None:
        return None
    return {
        k: (v.upper() if "COLOR" in k.upper() and v is not None else v)
        for k, v in options_map.items()
    }
uppercase_color_option_udf = udf(uppercase_color_option, selections_schema)

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()

initial_data = snowflake.execute_reader(queries.product_configs).cache()


initial_data = initial_data.withColumn("PRODUCT_VERSION", initial_data.PRODUCT_VERSION.cast('integer'))
initial_data = initial_data.withColumn("MIN_QUANTITY", initial_data.MIN_QUANTITY.cast('integer'))
initial_data = initial_data.withColumn("USER_SELECTIONS", from_json(col("USER_SELECTIONS"), selections_schema))
initial_data = initial_data.withColumn("USER_SELECTIONS_CONFIG_ONLY", expr("map_filter(USER_SELECTIONS, (k, v) -> k != 'Size')"))
initial_data = initial_data.withColumn("USER_SELECTIONS_CONFIG_ONLY_SOURCE", uppercase_color_option_udf(col("USER_SELECTIONS_CONFIG_ONLY")))
initial_data = initial_data.withColumn("INDEX", f.sha2(f.concat(*(f.col(c).cast("string") for c in ["PRODUCT_KEY", "MARKET","USER_SELECTIONS_CONFIG_ONLY_SOURCE"])), 256))
initial_data = initial_data.withColumn("USER_SELECTIONS_CONFIG_ONLY_SOURCE_STR", to_json(col("USER_SELECTIONS_CONFIG_ONLY_SOURCE"))) 
configs_df = initial_data.dropDuplicates(["INDEX", "USER_SELECTIONS_CONFIG_ONLY_SOURCE_STR"])
configs_df = configs_df.withColumn("USER_SELECTIONS_CONFIG_ONLY_SOURCE", from_json(col("USER_SELECTIONS_CONFIG_ONLY_SOURCE_STR"), selections_schema))
configs_df = configs_df.drop(*["USER_SELECTIONS","USER_SELECTIONS_CONFIG_ONLY_SOURCE_STR",'SIZE'])
configs_df = configs_df.withColumn("EXACT_COLOR_SORTED", array_join(array_sort(split(col("EXACT_COLOR"), "/")), "/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify constraints

# COMMAND ----------

raw_constraints_df = snowflake.execute_reader(queries.constraints_query).cache()
raw_constraints_df = raw_constraints_df.withColumn("SOURCE_COLOR_SORTED", array_join(array_sort(split(col("SOURCE_COLOR"), "/")), "/"))

raw_constraints_df = raw_constraints_df.join(configs_df, (raw_constraints_df.SOURCE_PRODUCT_KEY == configs_df.PRODUCT_KEY) & (raw_constraints_df.SOURCE_PRODUCT_VERSION == configs_df.PRODUCT_VERSION) & (raw_constraints_df.SOURCE_MARKET == configs_df.MARKET), "inner")

final_constraints_df = raw_constraints_df.select('SOURCE_PRODUCT_KEY','SOURCE_PRODUCT_VERSION','SOURCE_MARKET','SOURCE_COLOR_SORTED','INDEX','EXACT_COLOR_SORTED')
# flag to identify constrained configs
final_constraints_df = final_constraints_df.withColumn("CONFIG_CONSTRAINT", when((col("SOURCE_COLOR_SORTED") == "all") | (col("SOURCE_COLOR_SORTED") == col("EXACT_COLOR_SORTED")), 1).otherwise(0))

final_constraints_df = final_constraints_df.where(col("CONFIG_CONSTRAINT") == 1)

# COMMAND ----------

final_constraints_df = final_constraints_df.drop('SOURCE_PRODUCT_KEY', 'SOURCE_PRODUCT_VERSION', 'SOURCE_MARKET', 'SOURCE_COLOR_SORTED' ,  'EXACT_COLOR_SORTED')

configs_df = configs_df.join(final_constraints_df, ['INDEX'], "left").withColumn("CONFIG_CONSTRAINT", coalesce("CONFIG_CONSTRAINT",lit(0))).drop('EXACT_COLOR_SORTED')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning

# COMMAND ----------

gender_in_name_vals = [" MEN", "WOMEN", "LADIE", "UNISEX", "KID", "YOUTH"]
regex_pattern = "|".join(gender_in_name_vals)

configs_df.where(
    col("GENDER").isNull() & 
    upper(col("PRODUCT_NAME")).rlike(regex_pattern)
).display()

# COMMAND ----------

# use available gender from name if attribute is null
configs_df = configs_df.withColumn(
    "GENDER",
    when(
        (col("GENDER").isNull()) & (upper(col("PRODUCT_NAME")).contains(" MEN")) | (upper(col("PRODUCT_NAME")).contains("(MEN")),
        "MEN"
    ).when(
        (col("GENDER").isNull()) & (upper(col("PRODUCT_NAME")).contains("WOMEN")) | (upper(col("PRODUCT_NAME")).contains("LADIE")),
        "WOMEN"
    ).when(
        (col("GENDER").isNull()) & (upper(col("PRODUCT_NAME")).contains("UNISEX")),
        "MEN OR WOMEN"
    ).when(
        (col("GENDER").isNull()) & (upper(col("PRODUCT_NAME")).contains("KID")) | (upper(col("PRODUCT_NAME")).contains("YOUTH")),
        "YOUTH"
    ).otherwise(col("GENDER"))
)

# COMMAND ----------

# TYPE
type_values_cleaning = {'BASEBALL CAPS': "CAP",
                        'BASEBALL CAP': "CAP",
                        'TRUCKER CAPS': "CAP",}
configs_df = configs_df.replace(type_values_cleaning)

# COMMAND ----------

panels_median = configs_df.groupBy("MARKET","TYPE").agg(expr("percentile_approx(PANELS, 0.5)").alias("PANELS_MEDIAN"))
panels_median.display()

# COMMAND ----------

# Fill null panels values with median of same type & market  
configs_df = configs_df.join(panels_median, on=["MARKET", "TYPE"], how="left")
configs_df = configs_df.withColumn("PANELS", when(col("PANELS").isNull(), col("PANELS_MEDIAN")).otherwise(col("PANELS")))
configs_df = configs_df.drop("PANELS_MEDIAN")
configs_df = configs_df.na.fill(0, subset=["PANELS"])

# COMMAND ----------

# GENDER
gender_values_cleaning = {
    "Ladies'": "WOMEN",
    "MEN/UNISEX": "MEN OR WOMEN",
    "MEN,MEN/UNISEX": "MEN OR WOMEN",
    "MEN/UNISEX,MEN": "MEN OR WOMEN",
    "Men's": "MEN",
    "MALE": "MEN",
    "Kids": "YOUTH",
    "WOMENS": "WOMEN",
    "UNISEX": "MEN OR WOMEN",
    "ADULT": "MEN OR WOMEN",
    "UNISEX, WOMEN, MEN": "MEN OR WOMEN",
    "UNISEX, WOMEN , MEN": "MEN OR WOMEN",
    "UNISEX, MEN, WOMEN": "MEN OR WOMEN",
    "MEN, WOMEN, UNISEX": "MEN OR WOMEN"
}
configs_df = configs_df.replace(gender_values_cleaning)

# DECO TECH
deco_tech_cleaning = {
    'FULLCOLORTRANSFER': 'DIRECT GARMENT PRINT',
    'DIRECTGARMENTPRINT': 'DIRECT GARMENT PRINT',
    'DIRECTTOGARMENT': 'DIRECT GARMENT PRINT',
    'Direct-to-Garment': 'DIRECT GARMENT PRINT',
    'Textured Direct-to-Garment': 'DIRECT GARMENT PRINT',
    'COLORPRINT': 'DIRECT GARMENT PRINT',
    'FULLCOLORPRINT': 'DIRECT GARMENT PRINT',
    'HEATTRANSFER': 'HEAT TRANSFER',
    'EMBROIDERYFLAT': 'EMBROIDERY',
    'EMBROIDERYCYLINDER': 'EMBROIDERY'
}
configs_df = configs_df.replace(deco_tech_cleaning)

# DECO LOCATION
deco_location_cleaning = {'LC': 'LEFT CHEST'}
configs_df = configs_df.replace(deco_location_cleaning)

# BACKSIDE
configs_df = configs_df.withColumn("BACKSIDE", when(col("BACKSIDE") == "blank", "Blank").otherwise(col("BACKSIDE")))

# QUALITY
quality_values_cleaning = {'C': None}
configs_df = configs_df.replace(quality_values_cleaning)

# COLLAR - remove 'collar' / 'neck' from description 
configs_df = configs_df.withColumn("COLLAR_TYPE", regexp_replace(col("COLLAR_TYPE"), r"(?i)\s*COLLAR\s*|\s*NECK\s*|-NECK\s*", ""))

# PANELS & BACK POCKETS -> INTS w/ 0 as fill
configs_df = configs_df.withColumn("PANELS", when(col("PANELS") == "NOT APPLICABLE", 0).otherwise(col("PANELS")))
configs_df = configs_df.withColumn("PANELS", configs_df.PANELS.cast(IntegerType()))
configs_df = configs_df.withColumn("BACK_POCKET_S", configs_df.BACK_POCKET_S.cast(IntegerType()))
configs_df = configs_df.na.fill(0, subset=["PANELS", "BACK_POCKET_S"])


# COLOR
configs_df = configs_df.withColumn("L_COLOR", hex_to_lab_udf(col("COLOR"))[0]) \
                           .withColumn("A_COLOR", hex_to_lab_udf(col("COLOR"))[1]) \
                           .withColumn("B_COLOR", hex_to_lab_udf(col("COLOR"))[2])

configs_df = configs_df.withColumn("COLOR_NAME", closest_color_name_udf(col("COLOR")))


# Cols to replace commas with comma w/ space for readability
space_cols_to_clean = ["FIT", "POCKETS", "SPECIAL_FEATURES", "FRONT_POCKET_CLOSURES", "TYPE", "CHEST_POCKET_TYPE", "CUFF_TYPE", "BRIM", "EYELETS", "DECORATION_LOCATION_1"]
for c in space_cols_to_clean:
    configs_df = configs_df.withColumn(c, regexp_replace(c, ',', ', '))

# Parenthesis columns
parenthesis_cols_to_clean = ["POCKETS", "SPECIAL_FEATURES"]
for c in parenthesis_cols_to_clean:
    configs_df = configs_df.withColumn(c, regexp_replace(c, r"[()]", ""))


# Replace any variation of 'none' with nulls
validation_cols_to_exclude = ['INDEX','PRODUCT_KEY', 'PRODUCT_VERSION', 'MARKET', 'RECS_MODEL_REGION', 'PRODUCT_NAME', 'CATEGORY', 'SUBCATEGORY', 'PRODUCT_GROUP','USER_SELECTIONS', 'USER_SELECTIONS_CONFIG_ONLY', 'USER_SELECTIONS_CONFIG_ONLY_SOURCE', 'L_COLOR', 'A_COLOR', 'B_COLOR', 'SIZE', 'COLOR_NAME']

cols_to_validate = [i for i in configs_df.columns if i not in validation_cols_to_exclude]
for c in cols_to_validate:
    configs_df = configs_df.withColumn(
        c,
        when(col(c).rlike("(?i)^none$"), None).otherwise(col(c))
    )


configs_df = configs_df.cache()

# COMMAND ----------

# user selected options only (no size)
configs_count = configs_df.count()
print(f"Configs total: {configs_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config Qualification

# COMMAND ----------

try:
    config_embeddings_df_current = spark.read.format("delta").table(f"vista.{unity_schema}.{current_embedding_table_name}")

    print('Configs delta table exists. Reading data')
except:
    configs_df_schema = StructType.fromJson(json.loads(configs_df.schema.json()))
    delta_table_schema = configs_df_schema.add(StructField("FULL_EMBEDDING", ArrayType(FloatType(), True))).add(StructField("CREATED_AT", TimestampType(), True))
    config_embeddings_df_current = spark.createDataFrame([], delta_table_schema)

    config_embeddings_df_current \
    .write \
    .mode("overwrite") \
    .saveAsTable(f"vista.{unity_schema}.{current_embedding_table_name}")

    print('Configs delta table does not exist. Creating empty table')

# COMMAND ----------

configs_df_daily = configs_df.withColumn("FULL_EMBEDDING", lit(None).cast(ArrayType(FloatType()))) \
                            .withColumn("CREATED_AT", lit(None).cast(TimestampType()))

configs_df_daily = configs_df_daily.dropDuplicates(["INDEX"])                           

configs_df_daily \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"vista.{unity_schema}.{region_daily_embedding_table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete configs that are no longer available (not in daily query)
# MAGIC MERGE INTO IDENTIFIER('vista.' || :unity_schema_widget || '.ps_config_embeddings') target
# MAGIC USING IDENTIFIER('vista.' || :unity_schema_widget || '.' || :region_daily_embedding_table_name_widget) source
# MAGIC ON source.INDEX = target.INDEX
# MAGIC WHEN NOT MATCHED BY SOURCE AND target.recs_model_region = :recs_region THEN
# MAGIC   DELETE

# COMMAND ----------

curr_cols_to_rename = config_embeddings_df_current.columns
for i in range(len(curr_cols_to_rename)):
   config_embeddings_df_current=config_embeddings_df_current.withColumnRenamed(curr_cols_to_rename[i],
                                             'CURRENT_'+ curr_cols_to_rename[i])

# COMMAND ----------

new_configs_df = configs_df.join(config_embeddings_df_current, on=[configs_df.INDEX == config_embeddings_df_current.CURRENT_INDEX], how='left')

# COMMAND ----------

new_configs_df = new_configs_df.withColumn(
    "new_product_version_flag",
    when(new_configs_df.PRODUCT_VERSION != coalesce(new_configs_df.CURRENT_PRODUCT_VERSION, lit(-999)), 1).otherwise(0)
).withColumn(
    "price_change_flag",
    when(abs((new_configs_df.MOQ_UNIT_PRICE - new_configs_df.CURRENT_MOQ_UNIT_PRICE) /
              coalesce(new_configs_df.CURRENT_MOQ_UNIT_PRICE, lit(999999))) >= 0.05, 1).otherwise(0)
).withColumn(
    "different_product_info_flag",
    when(reduce(lambda x, y: x | 
                    (
                        lit(True) if f"CURRENT_{y}" not in new_configs_df.columns
                    else
                        new_configs_df[y] != coalesce(new_configs_df[f"CURRENT_{y}"], lit(999999))
                    ), cols_to_validate, lit(False)), 1).otherwise(0)).withColumn(
    "recalculate_embedding_flag",
    when((col("new_product_version_flag") == 1) | (col("price_change_flag") == 1) | (col("different_product_info_flag") == 1), 1).otherwise(0)
)

# COMMAND ----------

new_configs_df = new_configs_df.where(new_configs_df.recalculate_embedding_flag == 1)
new_configs_df.display()

# COMMAND ----------

new_configs_df = new_configs_df.drop(*config_embeddings_df_current.columns, 'new_product_version_flag', 'price_change_flag', 'different_product_info_flag','recalculate_embedding_flag').cache()

new_configs_pd = new_configs_df.toPandas()

# COMMAND ----------

new_configs_count = new_configs_df.count()


print(f"""
      Configs to generate new embeddings: {new_configs_count}
      Configs with no changes: {configs_count - new_configs_count} 
      """)

# COMMAND ----------

if new_configs_count == 0:
    dbutils.notebook.exit("No new configs to process")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null value check

# COMMAND ----------

null_condition = reduce(lambda x, y: x | y, [col(c).isNull() | (col(c) == '') for c in cols_to_validate])
rows_with_nulls = new_configs_df.filter(null_condition)
null_count = rows_with_nulls.count()

print(f"Configs will null/blank data: {null_count}")

# COMMAND ----------

rows_with_nulls.display()

# COMMAND ----------

nulls_counts_df = new_configs_df.groupBy(['PRODUCT_GROUP']).agg(*[count(new_configs_df.INDEX)] + [null_counts(c) for c in cols_to_validate]).withColumnRenamed('count(INDEX)', 'CONFIG_COUNT' )
nulls_counts_df.display()

# COMMAND ----------

# assert null_count < 100, "Warning: high value of null attribute features. Check PIM for naming convention changes."

# COMMAND ----------

# MAGIC %md
# MAGIC ##Genderate Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text Embeddings

# COMMAND ----------

emd_dim_dict = {'text_type': 18,
                'text_gender': 18,
                'text_sleeve': 12,
                'text_other': 12,
                'numerical_pc': 10,
                'numerical_other': 4,
                'categorical': 4}

# COMMAND ----------

type_df = new_configs_df.withColumn(
    "TYPE_TEXT",
    concat_ws(
        " ",
        #when(col("SUBCATEGORY").isNotNull(), concat(lit("The category is "), col("SUBCATEGORY"), lit("."))).otherwise(lit("")),
        #when(col("PRODUCT_GROUP").isNotNull(), concat(lit("The product group is "), col("PRODUCT_GROUP"), lit("."))).otherwise(lit("")),
        when(col("TYPE").isNotNull(), concat(lit("The product type is a "), col("TYPE"), lit("."))).otherwise(lit(""))
    )
).select("INDEX", "TYPE_TEXT").repartition(200)

# COMMAND ----------

model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=emd_dim_dict['text_type']).cpu()
type_embedding_df = type_df.withColumn("TYPE_EMBEDDINGS", embed_udf("TYPE_TEXT")).cache()

# COMMAND ----------

# Columns to check for nulls
gender_null_check = ["GENDER"]
gender_df = new_configs_df.withColumn(
    "GENDER_TEXT",
    when(
        # Check if gender is null
        reduce(lambda x, y: x | y, (col(c).isNull() for c in gender_null_check)),
        lit("Gender is not relevant.")
    ).otherwise(
        # Generate the summary text if no nulls
        concat_ws(
            " ",
            when(col("GENDER").isNotNull(), concat(lit("The "), col("TYPE"),  lit(" is specifically intended for "), configs_df["GENDER"], lit(". ")))
        )
    )
).select("INDEX", "GENDER_TEXT").repartition(200)

# COMMAND ----------

model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=emd_dim_dict['text_gender']).cpu()
gender_embedding_df = gender_df.withColumn("GENDER_EMBEDDINGS", embed_udf("GENDER_TEXT")).cache()

# COMMAND ----------

# Columns to check for nulls
sleeve_null_check = ["SLEEVE_STYLE"]
sleeve_df = new_configs_df.withColumn(
    "SLEEVE_TEXT",
    when(
        # Check if sleeve is null
        reduce(lambda x, y: x | y, (col(c).isNull() for c in sleeve_null_check)),
        lit("Sleeves are not relevant.")
    ).otherwise(
        # Generate the summary text if no nulls
        concat_ws(
            " ",
            when(col("SLEEVE_STYLE").isNotNull(), concat(lit("The sleeve style is "), configs_df["SLEEVE_STYLE"], lit(". "))  
        )
    )
)).select("INDEX", "SLEEVE_TEXT").repartition(200)

# COMMAND ----------

model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=emd_dim_dict['text_sleeve']).cpu()
sleeve_embedding_df = sleeve_df.withColumn("SLEEVE_EMBEDDINGS", embed_udf("SLEEVE_TEXT")).cache()

# COMMAND ----------

# Columns to check for nulls
other_text_null_check = ["DECORATION_LOCATION_1",'DECORATION_TECHNOLOGY_1','MATERIAL','COLLAR_FASTENER','COLLAR_TYPE','FIT','POCKETS','SPECIAL_FEATURES','FRONT_POCKET_CLOSURES','QUALITY','WAIST_TYPE','FRONT_POCKET_CLOSURE','CHEST_POCKET_TYPE','CUFF_TYPE','BRIM','EYELETS','CROWN','CLOSURE','FIT_TYPE','FASTNER_TYPE']
other_text_df = new_configs_df.withColumn(
    "OTHER_TEXT",
    when(
        # Check if sleeve is null
        reduce(lambda x, y: x & y, (col(c).isNull() for c in other_text_null_check)),
        lit("No additional information.")
    ).otherwise(
        # Generate the summary text if no nulls
           concat_ws(
        " ",
        when(col("DECORATION_LOCATION_1").isNotNull(), concat(lit("The decoration area is "), col("DECORATION_LOCATION_1"), lit("."))).otherwise(lit("")),
        when(col("DECORATION_TECHNOLOGY_1").isNotNull(), concat(lit("The decoration style is "), col("DECORATION_TECHNOLOGY_1"), lit("."))).otherwise(lit("")),
        when(col("MATERIAL").isNotNull(), concat(lit("The material is "), col("MATERIAL"), lit("."))).otherwise(lit("")),
        when(col("COLLAR_FASTENER").isNotNull(), concat(lit("The collar has "), col("COLLAR_FASTENER"), lit(" fastener. "))).otherwise(lit("")),
        when(col("COLLAR_TYPE").isNotNull(), concat(lit("The collar style is  "), col("COLLAR_TYPE"), lit("."))).otherwise(lit("")),
        when(col("FIT").isNotNull(), concat(lit("The fit of the "), col("TYPE"), lit(" is "), col("FIT"), lit("."))).otherwise(lit("")),
        when(col("POCKETS").isNotNull(), concat(lit("The pockets available are "), col("POCKETS"), lit("."))).otherwise(lit("")),
        when(col("SPECIAL_FEATURES").isNotNull(), concat(lit("The "), col("TYPE"), lit(" following special features: "), col("SPECIAL_FEATURES"), lit("."))).otherwise(lit("")),
        when(col("FRONT_POCKET_CLOSURES").isNotNull(), concat(lit("The "), col("TYPE"), lit(" pocket type is "), col("FRONT_POCKET_CLOSURES"), lit("."))).otherwise(lit("")),

        when(col("QUALITY").isNotNull(), concat(lit("The quality is "), col("QUALITY"), lit("."))).otherwise(lit("")),
        when(col("WAIST_TYPE").isNotNull(), concat(lit("The "), col("TYPE"), lit(" waist is an "), col("WAIST_TYPE"), lit("."))).otherwise(lit("")),
        when(col("FRONT_POCKET_CLOSURE").isNotNull(), concat(lit("The "), col("TYPE"), lit(" front pocket type is "), col("FRONT_POCKET_CLOSURE"), lit("."))).otherwise(lit("")),
        when(col("CHEST_POCKET_TYPE").isNotNull(), concat(lit("The chest pocket type is "), col("CHEST_POCKET_TYPE"), lit(" on the "), col("CHEST_POCKET_S"), lit("."))).otherwise(lit("")),
        when(col("CUFF_TYPE").isNotNull(), concat(lit("The cuff type is "), col("CUFF_TYPE"), lit("."))).otherwise(lit("")),
        when(col("BRIM").isNotNull(), concat(lit("The "), col("TYPE"), lit(" brim type is "),col("BRIM"), lit("."))).otherwise(lit("")),
        when(col("EYELETS").isNotNull(), concat(lit("The "), col("TYPE"), lit(" eyelets are "),col("EYELETS"), lit("."))).otherwise(lit("")),
        when(col("CROWN").isNotNull(), concat(lit("The "), col("TYPE"), lit(" crown is "),col("CROWN"), lit("."))).otherwise(lit("")),
        when(col("CLOSURE").isNotNull(), concat(lit("The "), col("TYPE"), lit(" closure is "),col("CLOSURE"), lit("."))).otherwise(lit("")),
        when(col("FIT_TYPE").isNotNull(), concat(lit("The "), col("TYPE"), lit(" fit is "),col("FIT_TYPE"), lit("."))).otherwise(lit("")),
        when(col("FASTNER_TYPE").isNotNull(), concat(lit("The "), col("FASTNER_TYPE"), lit(" fastner is "),col("CROWN"), lit("."))).otherwise(lit("")),
        when(col("ZIPPER_PULLS").isNotNull(), concat(lit("The "), col("TYPE"), lit(" has zippers."))).otherwise(lit("")),
        
)
)).select("INDEX", "OTHER_TEXT").repartition(200)

# COMMAND ----------

model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=emd_dim_dict['text_other']).cpu()
other_text_embedding_df = other_text_df.withColumn("OTHER_TEXT_EMBEDDINGS", embed_udf("OTHER_TEXT")).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical Embeddings

# COMMAND ----------

# scaler_moq = StandardScaler()
num_pc_embedding_dim = emd_dim_dict['numerical_pc']

scaler_price = StandardScaler()
scaler_l = StandardScaler()
scaler_a = StandardScaler()
scaler_b = StandardScaler()


# Assuming you have your data stored in a DataFrame called 'items'
# Normalize numerical features

new_configs_pd['MOQ_UNIT_PRICE_SCALED'] = scaler_price.fit_transform(new_configs_pd['MOQ_UNIT_PRICE'].values.reshape(-1, 1))
new_configs_pd['L_COLOR_SCALED'] = scaler_l.fit_transform(new_configs_pd['L_COLOR'].values.reshape(-1, 1))
new_configs_pd['A_COLOR_SCALED'] = scaler_a.fit_transform(new_configs_pd['A_COLOR'].values.reshape(-1, 1))
new_configs_pd['B_COLOR_SCALED'] = scaler_b.fit_transform(new_configs_pd['B_COLOR'].values.reshape(-1, 1))

#input_moq = Input(shape=(1,), name='input_moq')
input_price = Input(shape=(1,), name='input_moq')
input_l_color = Input(shape=(1,), name='input_l')
input_a_color = Input(shape=(1,), name='input_a')
input_b_color = Input(shape=(1,), name='input_b')

num_pc_concatenated_inputs = Concatenate()([input_price, input_l_color, input_a_color, input_b_color])
num_pc_dense_layer = Dense(num_pc_embedding_dim, activation='relu')(num_pc_concatenated_inputs)
num_pc_output_layer = Dense(num_pc_embedding_dim, activation='linear')(num_pc_dense_layer)

num_pc_model = Model(inputs=[input_price,input_l_color, input_a_color, input_b_color], outputs= num_pc_output_layer)

# Compile the model
num_pc_model.compile(optimizer='adam', loss='mse')

# Extract embeddings for your data
num_pc_encoder_model = Model(inputs=num_pc_model.input, outputs=num_pc_model.layers[-2].output)

num_pc_embeddings = num_pc_encoder_model.predict([new_configs_pd['MOQ_UNIT_PRICE_SCALED'], new_configs_pd['L_COLOR_SCALED'], new_configs_pd['A_COLOR_SCALED'], new_configs_pd['B_COLOR_SCALED']])

# Add embeddings to the DataFrame
new_configs_pd['NUMERICAL_EMBEDDINGS_PC'] = list(num_pc_embeddings)
new_configs_pd['NUMERICAL_EMBEDDINGS_PC'] = new_configs_pd['NUMERICAL_EMBEDDINGS_PC'].apply(lambda x: x.tolist())
new_configs_pd['NUMERICAL_EMBEDDINGS_PC'] = new_configs_pd['NUMERICAL_EMBEDDINGS_PC'].apply(json.dumps)

# COMMAND ----------

# scaler_moq = StandardScaler()
num_other_embedding_dim = emd_dim_dict['numerical_other']

scaler_back_pockets = StandardScaler()
scaler_panels = StandardScaler()


# Assuming you have your data stored in a DataFrame called 'items'
# Normalize numerical features

new_configs_pd['BACK_POCKET_S_SCALED'] = scaler_back_pockets.fit_transform(new_configs_pd['BACK_POCKET_S'].values.reshape(-1, 1))
new_configs_pd['PANELS_SCALED'] = scaler_panels.fit_transform(new_configs_pd['PANELS'].values.reshape(-1, 1))

input_back_pockets = Input(shape=(1,), name='input_back_pockets')
input_panels = Input(shape=(1,), name='input_panels')

num_other_concatenated_inputs = Concatenate()([input_back_pockets, input_panels])
num_other_dense_layer = Dense(num_other_embedding_dim, activation='relu')(num_other_concatenated_inputs)
num_other_output_layer = Dense(num_other_embedding_dim, activation='linear')(num_other_dense_layer)

num_other_model = Model(inputs=[input_back_pockets,input_panels], outputs= num_other_output_layer)

# Compile the model
num_other_model.compile(optimizer='adam', loss='mse')

# Extract embeddings for your data
num_other_encoder_model = Model(inputs=num_other_model.input, outputs=num_other_model.layers[-2].output)

num_other_embeddings = num_other_encoder_model.predict([new_configs_pd['BACK_POCKET_S_SCALED'], new_configs_pd['PANELS_SCALED']])

# Add embeddings to the DataFrame
new_configs_pd['NUMERICAL_EMBEDDINGS_OTHER'] = list(num_other_embeddings)
new_configs_pd['NUMERICAL_EMBEDDINGS_OTHER'] = new_configs_pd['NUMERICAL_EMBEDDINGS_OTHER'].apply(lambda x: x.tolist())
new_configs_pd['NUMERICAL_EMBEDDINGS_OTHER'] = new_configs_pd['NUMERICAL_EMBEDDINGS_OTHER'].apply(json.dumps)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Categorical Embeddings

# COMMAND ----------

lista = []
lista_inputs = []
for i in ['BACKSIDE']: 
  encoder = LabelEncoder()
  new_configs_pd[i+'_ENCODED'] = encoder.fit_transform(new_configs_pd[i])
  num_categories = len(encoder.classes_)
  cat_embedding_dim = emd_dim_dict['categorical']

  # Define inputs
  input_name = i.lower() + '_input'
  category_input = Input(shape=(1,), name=input_name)
  lista_inputs.append(category_input)
  # Define embedding layers
  input_embedding = i.lower() + '_embedding'
  category_embedding = Embedding(input_dim=num_categories, output_dim=cat_embedding_dim, name=input_embedding)(category_input)

  # Flatten the embeddings
  category_embedding = Flatten()(category_embedding)
  lista.append(category_embedding)


# Concatenate embeddings
combined_embedding = Concatenate()(lista)

# Define the model
model = Model(inputs=lista_inputs, outputs=combined_embedding)
model.compile(optimizer='adam', loss='mse')  # Dummy loss for the sake of example

# Print the model summary
model.summary()
    

# COMMAND ----------

categorical_embeddings = model.predict([new_configs_pd['BACKSIDE_ENCODED'].values])

new_configs_pd['CATEGORICAL_EMBEDDINGS'] = list(categorical_embeddings)
new_configs_pd['CATEGORICAL_EMBEDDINGS'] = new_configs_pd['CATEGORICAL_EMBEDDINGS'].apply(lambda x: x.tolist())
new_configs_pd['CATEGORICAL_EMBEDDINGS'] = new_configs_pd['CATEGORICAL_EMBEDDINGS'].apply(json.dumps)

# COMMAND ----------

numerical_embedding_df = spark.createDataFrame(new_configs_pd)

numerical_embedding_df = numerical_embedding_df.select("INDEX", "NUMERICAL_EMBEDDINGS_PC", "NUMERICAL_EMBEDDINGS_OTHER", "CATEGORICAL_EMBEDDINGS")

# COMMAND ----------

numerical_embedding_df = spark.createDataFrame(new_configs_pd)
numerical_embedding_df = numerical_embedding_df.select("INDEX", "NUMERICAL_EMBEDDINGS_PC", "NUMERICAL_EMBEDDINGS_OTHER","CATEGORICAL_EMBEDDINGS")
array_schema = ArrayType(FloatType())
numerical_embedding_df = numerical_embedding_df.withColumn("NUMERICAL_EMBEDDINGS_PC", from_json(col("NUMERICAL_EMBEDDINGS_PC"), array_schema))
numerical_embedding_df = numerical_embedding_df.withColumn("NUMERICAL_EMBEDDINGS_OTHER", from_json(col("NUMERICAL_EMBEDDINGS_OTHER"), array_schema))
numerical_embedding_df = numerical_embedding_df.withColumn("CATEGORICAL_EMBEDDINGS", from_json(col("CATEGORICAL_EMBEDDINGS"), array_schema)).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Concat Embeddings

# COMMAND ----------

full_embedding_df = type_embedding_df.join(numerical_embedding_df, ["INDEX"])\
                           .join(gender_embedding_df, ["INDEX"])\
                            .join(sleeve_embedding_df,["INDEX"])\
                            .join(other_text_embedding_df,["INDEX"])


# COMMAND ----------

full_embedding_df = full_embedding_df.withColumn('FULL_EMBEDDING', flatten(array(col("TYPE_EMBEDDINGS"), col("NUMERICAL_EMBEDDINGS_PC"), col("NUMERICAL_EMBEDDINGS_OTHER"), col("CATEGORICAL_EMBEDDINGS"), col('GENDER_EMBEDDINGS'), col('SLEEVE_EMBEDDINGS'), col('OTHER_TEXT_EMBEDDINGS')))).select('INDEX','FULL_EMBEDDING').cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Out Embeddings

# COMMAND ----------

config_embeddings_df = configs_df.join(full_embedding_df, ["INDEX"]).withColumns({'CREATED_AT': current_timestamp()}).cache()

# COMMAND ----------

# unity 
config_embeddings_df \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"vista.{unity_schema}.{region_new_embeddings_table}")

# COMMAND ----------

# add any new columns to config embeddings table
source_table = spark.table(f"vista.{unity_schema}.{region_new_embeddings_table}")
target_table = spark.table(f"vista.{unity_schema}.ps_config_embeddings")

source_cols = set(source_table.columns)
target_cols = set(target_table.columns)

new_target_cols = source_cols - target_cols

for c in new_target_cols:
    data_type = [f"{f.name} {f.dataType.simpleString()}" for f in source_table.schema.fields if f.name == c][0]
    print(f"ALTER TABLE vista.{unity_schema}.ps_config_embeddings ADD COLUMNS ({data_type})")
    spark.sql(f"ALTER TABLE vista.{unity_schema}.ps_config_embeddings ADD COLUMNS ({data_type})")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO IDENTIFIER('vista.' || :unity_schema_widget || '.ps_config_embeddings') target
# MAGIC USING IDENTIFIER('vista.' || :unity_schema_widget || '.' || :region_new_embedding_table_name_widget) source
# MAGIC ON source.INDEX = target.INDEX
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

initial_data.unpersist()
configs_df.unpersist()
new_configs_df.unpersist()
type_embedding_df.unpersist()
gender_df.unpersist()
sleeve_df.unpersist()
other_text_df.unpersist()
numerical_embedding_df.unpersist()
config_embeddings_df.unpersist()
