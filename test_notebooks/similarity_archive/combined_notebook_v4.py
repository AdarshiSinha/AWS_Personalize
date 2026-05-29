# Databricks notebook source
# MAGIC %run ../setups/install_packages

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

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from angle_emb import AnglE, Prompts
from colormath.color_conversions import convert_color
from colormath.color_diff import delta_e_cmc
from colormath.color_objects import LabColor, sRGBColor
from pyspark.sql import SparkSession
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, round, when, lit, pow, current_timestamp, log, monotonically_increasing_id, row_number, concat, array, struct, rank, dense_rank
from pyspark.sql.types import *
from pyspark.sql.window import Window
from sentence_transformers import SentenceTransformer, util
from skimage.color import rgb2lab
from sklearn.metrics import ConfusionMatrixDisplay
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import LabelEncoder, StandardScaler
from tensorflow.keras.layers import TextVectorization, Embedding, Input, Flatten, Concatenate, Input, Dense, Concatenate
from tensorflow.keras.models import Model
from transformers import pipeline
import boto3
import io
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import sys
import tensorflow as tf
import torch
import webcolors

print("Python version:", sys.version)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

# Function to convert hexadecimal to RGB
def hex_to_rgb(hex_color):
    rgb = mcolors.hex2color(hex_color)
    return [int(x * 255) for x in rgb]

def normalize_rgb(rgb_color):
    return [x / 255.0 for x in rgb_color]

def rgb_to_lab(rgb_color):
    return rgb2lab([[rgb_color]])[0][0]

def hex_to_lab(hex_color):
    rgb_color = hex_to_rgb(hex_color)
    normalized_rgb = normalize_rgb(rgb_color)
    lab_color = rgb_to_lab(normalized_rgb)
    return lab_color

def hex_to_color_name(hex_code):
    try:
        color_name = webcolors.hex_to_name(hex_code)
        return color_name
    except ValueError:
        return "Unknown"
    

def closest_colour(requested_colour):
    min_colours = {}

    for key, name in webcolors.CSS3_HEX_TO_NAMES.items():
        rbg_requested_colour = webcolors.hex_to_rgb(requested_colour)
        r_c, g_c, b_c = webcolors.hex_to_rgb(key)
        rd = (r_c - rbg_requested_colour[0]) ** 2
        gd = (g_c - rbg_requested_colour[1]) ** 2
        bd = (b_c - rbg_requested_colour[2]) ** 2
        min_colours[(rd + gd + bd)] = name
    return min_colours[min(min_colours.keys())]

def get_colour_name(requested_colour):
    try:
        closest_name = actual_name = webcolors.hex_to_name(requested_colour)
    except ValueError:
        closest_name = closest_colour(requested_colour)
        actual_name = None

    if actual_name != None:
        return(actual_name)
    else: 
        return(closest_name)
    
def prepare_embedding(text_df,spark, col_to_embed, dimensions=512, culture = "en"):
        '''
        prepare_albert_embedding: Generate the embeddings from the texts:
          ------------------
           prepare_albert_embedding(self,text_df,model = "albert_large_uncased")

          Parameters:
          -----------
            text_df (spark.DataFrame): dataframe containing the texts inside the images
            model (str): the model to be used (default: albert_large_uncased)
          Returns:
          --------
            result (spark.DataFrame): dataframe containing the utm_id and their respective text embedding
        '''
        text_array = text_df.select(f"{col_to_embed}").rdd.flatMap(lambda x: x).collect()
        
        # angle = AnglE.from_pretrained('WhereIsAI/UAE-Large-V1', pooling_strategy='cls').cuda()
        # pipe = pipeline(model="WhereIsAI/UAE-Large-V1",pooling_strategy='cls',device=0)
        # text_embedding = pipe(text_array,batch_size=64)
        # text_embedding = angle.encode(text_array,batch_size=4)
        model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=dimensions).cpu()
        #angle = SentenceTransformer('WhereIsAI/UAE-Large-V1').cuda()
        text_embedding = model.encode(text_array)


        
        schema = StructType([
                    StructField("embeddings", ArrayType(FloatType()), True)])
        doc_vecs = text_embedding.tolist()
        b = spark.createDataFrame([(l,) for l in doc_vecs], schema)
        a = text_df.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
        b = b.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
        return a.join(b, a.row_idx == b.row_idx).\
             drop("row_idx")

def transform_embeddings(embeddings):
        '''
        transform_albert_embeddings: explode the list of embeddings into rows of vectors:
          ------------------
           transform_albert_embeddings(self,embeddings)

          Parameters:
          -----------
            embeddings (spark.DataFrame): dataframe containing the utm_id and their respective text embedding
          Returns:
          --------
            result (spark.DataFrame): dataframe with the exploded rows of vectors
        '''
        result = embeddings.withColumn("embeddings",f.explode("finished_embeddings"))
        result = result.drop("finished_embeddings")
        df1 = result.select("utm_id","date",*[f.col("embeddings")[i] for i in range(1024)])
        result = df1.groupby("utm_id","date").agg(f.array([f.mean(i) 
                            for i in df1.columns[2:]]).alias("embeddings"))
        return result.withColumn("features",array_to_vector("embeddings"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data

# COMMAND ----------

# aws_role_session = "dna_ppp_temp_session_gravity"
# aws_peresonalize_execution_role = 'arn:aws:iam::905666450942:role/service-role/AmazonPersonalize-ExecutionRole-1595956851346'
# aws_personalize_role = dbutils.secrets.get(scope="personalizationProductRecommender", key="awsPersonalizeAccountRole")
# aws_account_id = "905666450942"

# COMMAND ----------

# client = boto3.client('sts')
# StsResponse = client.assume_role(RoleArn=aws_personalize_role, RoleSessionName=aws_role_session)

# s3_client =boto3.client('s3',
#                      aws_access_key_id=StsResponse['Credentials']['AccessKeyId'],
#                         aws_secret_access_key=StsResponse['Credentials']['SecretAccessKey'],
#                         aws_session_token=StsResponse['Credentials']['SessionToken'])
# #from recommender.aws_service.s3_client import S3Client
# S3_BUCKET = "precs-product-recommendation-dev"

# COMMAND ----------

# latest_users_path = 'config/configurations.csv'
# obj = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_users_path)
# items = pd.read_csv(io.BytesIO(obj['Body'].read()))

# # items = items.drop(columns= 'SIZE')
# # items = items.drop_duplicates().reset_index(drop=True)

# COMMAND ----------

items_query = f"""
WITH configs AS (
    SELECT
        pc.product_key,
        pc.product_name,
        pc.category,
        pc.subcategory,
        pc.product_group,
        pe.COUNTRY_CODE,
        TRIM(pe.FULFILLMENT_OPTIONS_STD:Size, '"') AS Size,
        CASE
            WHEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Substrate Color", '"') IS NOT NULL THEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Substrate Color", '"')
            WHEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Substrate color", '"') IS NOT NULL THEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Substrate color", '"')
            WHEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Color", '"') IS NOT NULL THEN TRIM(pe.FULFILLMENT_OPTIONS_STD:"Color", '"')
            ELSE NULL
        END AS COLOUR,
        d.IS_CURRENT,
        TRIM(d.BRAND_NAME, '"') AS BRAND_NAME,
        TRIM(d.COLLAR_TYPE, '"') AS COLLAR_TYPE,
        TRIM(d.DECORATION_LOCATION_1, '"') AS DECORATION_LOCATION_1,
        IFF(TRIM(d.DECORATION_LOCATION_2, '"') = 'BACK', 1, 0) AS BACK,
        TRIM(d.DECORATION_TECHNOLOGY_1, '"') AS DECORATION_TECHNOLOGY_1,
        TRIM(d.GENDER, '"') AS GENDER,
        TRIM(d.MATERIAL, '"') AS MATERIAL,
        d.MIN_QUANTITY,
        TRIM(d.SLEEVE_STYLE, '"') AS SLEEVE_STYLE
    FROM
        VISTAPRINT.PRODUCT.FULFILLMENT_OPTIONS_CONFIG pe
    JOIN
        VISTAPRINT.PRODUCT.PRODUCT_CATEGORIZATION pc ON pe.product_key = pc.product_key
    LEFT JOIN (
        SELECT
            PRODUCT_KEY,
            IS_CURRENT,
            GET(BRAND_NAME, 0) AS BRAND_NAME,
            GET(COLLAR_TYPE, 0) AS COLLAR_TYPE,
            GET(DECORATION_LOCATION_1, 0) AS DECORATION_LOCATION_1,
            GET(DECORATION_LOCATION_2, 0) AS DECORATION_LOCATION_2,
            GET(DECORATION_TECHNOLOGY_1, 0) AS DECORATION_TECHNOLOGY_1,
            GET(GENDER, 0) AS GENDER,
            GET(MATERIAL, 0) AS MATERIAL,
            MIN_QUANTITY,
            GET(SLEEVE_STYLE, 0) AS SLEEVE_STYLE
        FROM
            VISTAPRINT.PRODUCT.MCP_SKU_PRODUCT_CONFIG
    ) AS d ON d.PRODUCT_KEY = pc.PRODUCT_KEY
    WHERE
        pc.product_group IN ('T-shirts', 'Polos') AND
        --RIGHT(pc.product_key, 1) IN ('W', 'R') AND
        pe.country_code = 'US' AND
        d.IS_CURRENT = TRUE
),
RankedProducts AS (
    SELECT
        cfg.*,
        lp.moq_unit_price,
        RANK() OVER (PARTITION BY lp.PRODUCTKEY, lp.COLOUR, lp.SIZE, lp.BACK ORDER BY lp.EFFECTIVEFROM_UTC DESC) AS rank
    FROM (
        SELECT
            lp.PRODUCTKEY,
            lp.MARKET,
            lp.EFFECTIVEFROM_UTC,
            TRIM(lp.ATTRIBUTES:"Substrate Color", '"') AS COLOUR,
            TRIM(lp.ATTRIBUTES:Size, '"') AS Size,
            IFF(TRIM(lp.ATTRIBUTES:Backside, '"') = 'Color', 1, 0) AS BACK,
            PARSE_JSON(LOWER(GET(lp.prices, TO_CHAR(lp.moq)))):unitlistprice:untaxed * br.rate AS moq_unit_price
        FROM
            DNA.PRICING.HISTORIC_LIST_PRICES_NEW_PLATFORM lp
        JOIN
            VISTAPRINT.TRANSACTIONS.DIM_BUDGET_RATES br ON lp.currency = br.currency_from
        WHERE
            br.date = DATE(DATEADD(day, -1, CURRENT_DATE)) AND
            br.currency_to = 'USD'
    ) AS lp
    RIGHT JOIN
        configs AS cfg ON cfg.product_key = lp.PRODUCTKEY
    AND lp.market = cfg.country_code
    AND cfg.COLOUR = lp.COLOUR
    AND cfg.Size = lp.Size
    AND cfg.BACK = lp.BACK
    QUALIFY rank = 1
)
SELECT DISTINCT
    product_key,
    product_name,
    category,
    subcategory,
    product_group,
    BRAND_NAME,
    COLLAR_TYPE,
    DECORATION_LOCATION_1,
    BACK,
    DECORATION_TECHNOLOGY_1,
    GENDER,
    MATERIAL,
    MIN_QUANTITY,
    SLEEVE_STYLE,
    Size,
    COLOUR,
    moq_unit_price
FROM
    RankedProducts
    where len(COLOUR) = 7
    and right(product_key,1) = 'W'
"""



# COMMAND ----------

initial_data = snowflake.execute_reader(items_query)
items = initial_data.toPandas()

# COMMAND ----------

initial_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning

# COMMAND ----------

items['BRAND_NAME'] = items['BRAND_NAME'].str.replace('®', '')

items['COLLAR_TYPE'] = items['COLLAR_TYPE'].str.replace(' COLLAR', '')
items['COLLAR_TYPE'] = items['COLLAR_TYPE'].str.replace(' NECK', '')
items['COLLAR_TYPE'] = items['COLLAR_TYPE'].str.replace('-NECK', '')
items['COLLAR_TYPE'] = items['COLLAR_TYPE'].str.replace(' COLLAR', '')

items['COLOUR'] = items['COLOUR'].str.split('/').str[0]


# Apply function to transform hexadecimal values to RGB
# Apply the hex_to_lab function and split the LAB values into separate columns
items[['L_COLOR', 'A_COLOR', 'B_COLOR']] = items['COLOUR'].apply(lambda x: pd.Series(hex_to_lab(x)))

# COMMAND ----------

items[['HUMAN_COLOR']] = items['COLOUR'].apply(lambda x: pd.Series(get_colour_name(x)))

# COMMAND ----------

# Define a mapping of size categories to numerical values
size_mapping = {'XXS': 1, 'XS': 2, 'S': 3, 'M': 4, 'L': 5, 'XL': 6, '2XL': 7, '3XL':8, '4XL': 9, '5XL':10}

# Apply mapping to the 'Size' column
#items['Size_numerical'] = items['SIZE'].map(size_mapping)

# COMMAND ----------

items_df = spark.createDataFrame(items)
items_df = items_df.na.fill('')
items_df = items_df.withColumn("INDEX", f.sha2(f.concat(*(f.col(c).cast("string") for c in ["PRODUCT_KEY", "COLOUR"])), 256))
items_pd = items_df.toPandas()
configs_df = items_df.drop(items_df.SIZE).dropDuplicates()
configs_pd = configs_df.toPandas()
configs_pd.head()

# COMMAND ----------

configs_df.where(configs_df.GENDER =='YOUTH').display()

# COMMAND ----------

configs_df.count()

# COMMAND ----------

feature_tier_weight = {'tier_1': 1000,
                       'tier_2': 200,
                       'tier_3': 16,
                       'tier_4': 8}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 1

# COMMAND ----------

tier_1_text_cols = ['SUBCATEGORY', 'PRODUCT_GROUP'] #PRODUCT NAME?? 'PRODUCT_NAME',
tier_1_text_dim = feature_tier_weight['tier_1']

#logic for concat
for c in tier_1_text_cols:
    print(f"lit(\"The {c.replace('_', ' ')} is \"), configs_df[\"{c}\"], lit(\". \"),")

# COMMAND ----------

tier_1_text_df = configs_df.withColumn("COMBINED_TIER_1_TEXT",
                    # Tier 1 (8)
                    concat(lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The SUBCATEGORY is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The PRODUCT GROUP is "), configs_df["PRODUCT_GROUP"], lit(". "),
                    # Tier 2 (6)
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                        lit("The GENDER is "), configs_df["GENDER"], lit(". "),
                    # Tier 3 (4)
                       lit("The SLEEVE STYLE is "), configs_df["SLEEVE_STYLE"], lit(". "),
                        lit("The BRAND NAME is "), configs_df["BRAND_NAME"], lit(". "),
                        lit("The DECORATION LOCATION 1 is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                        lit("The DECORATION TECHNOLOGY 1 is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". "),
                        lit("The SLEEVE STYLE is "), configs_df["SLEEVE_STYLE"], lit(". "),
                        lit("The BRAND NAME is "), configs_df["BRAND_NAME"], lit(". "),
                        lit("The DECORATION LOCATION 1 is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                        lit("The DECORATION TECHNOLOGY 1 is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". "),
                        lit("The SLEEVE STYLE is "), configs_df["SLEEVE_STYLE"], lit(". "),
                        lit("The BRAND NAME is "), configs_df["BRAND_NAME"], lit(". "),
                        lit("The DECORATION LOCATION 1 is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                        lit("The DECORATION TECHNOLOGY 1 is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". "),
                        lit("The SLEEVE STYLE is "), configs_df["SLEEVE_STYLE"], lit(". "),
                        lit("The BRAND NAME is "), configs_df["BRAND_NAME"], lit(". "),
                        lit("The DECORATION LOCATION 1 is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                        lit("The DECORATION TECHNOLOGY 1 is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". "),
                    # Tier 4 (2)
                        lit("The COLLAR TYPE is "), configs_df["COLLAR_TYPE"], lit(". "),
                        lit("The MATERIAL is "), configs_df["MATERIAL"], lit(". "),
                        lit("The COLLAR TYPE is "), configs_df["COLLAR_TYPE"], lit(". "),
                        lit("The MATERIAL is "), configs_df["MATERIAL"], lit(". ")
                        
                   ))

# COMMAND ----------

tier_1_text_embedding_df = prepare_embedding(tier_1_text_df, spark, col_to_embed='COMBINED_TIER_1_TEXT', dimensions=tier_1_text_dim)
tier_1_text_embedding_df = tier_1_text_embedding_df.withColumnRenamed('embeddings', 'TIER_1_TEXT_EMBEDDINGS')
tier_1_text_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 2

# COMMAND ----------

tier_2_text_cols = ['GENDER']
tier_2_text_dim = feature_tier_weight['tier_2']


#logic for concat
for c in tier_2_text_cols:
    print(f"lit(\"The {c.replace('_', ' ')} is \"), configs_df[\"{c}\"], lit(\". \"),")

# COMMAND ----------

tier_2_text_df = configs_df.withColumn("COMBINED_TIER_2_TEXT",
                    concat(
                       lit("The GENDER is "), configs_df["GENDER"], lit(". ")
                   ))

# COMMAND ----------

tier_2_text_embedding_df = prepare_embedding(tier_2_text_df, spark, col_to_embed='COMBINED_TIER_2_TEXT', dimensions=tier_2_text_dim)
tier_2_text_embedding_df = tier_2_text_embedding_df.select(tier_2_text_embedding_df.INDEX, tier_2_text_embedding_df.embeddings.alias("TIER_2_TEXT_EMBEDDINGS"))
tier_2_text_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 3

# COMMAND ----------

tier_3_text_cols = ['SLEEVE_STYLE','BRAND_NAME','DECORATION_LOCATION_1','DECORATION_TECHNOLOGY_1']
tier_3_text_dim = feature_tier_weight['tier_3']

#logic for concat
for c in tier_3_text_cols:
    print(f"lit(\"The {c.replace('_', ' ')} is \"), configs_df[\"{c}\"], lit(\". \"),")

# COMMAND ----------

tier_3_text_df = configs_df.withColumn("COMBINED_TIER_3_TEXT",
                    concat(
                        lit("The BRAND NAME is "), configs_df["BRAND_NAME"], lit(". "),
                        lit("The DECORATION LOCATION 1 is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                        lit("The DECORATION TECHNOLOGY 1 is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". ")
                   ))

# COMMAND ----------

tier_3_text_embedding_df = prepare_embedding(tier_3_text_df, spark, col_to_embed='COMBINED_TIER_3_TEXT', dimensions=tier_3_text_dim)
tier_3_text_embedding_df = tier_3_text_embedding_df.select(tier_3_text_embedding_df.INDEX, tier_3_text_embedding_df.embeddings.alias("TIER_3_TEXT_EMBEDDINGS"))
tier_3_text_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 4

# COMMAND ----------

tier_4_text_cols = ['COLLAR_TYPE','MATERIAL']
tier_4_text_dim = feature_tier_weight['tier_4']

#logic for concat
for c in tier_4_text_cols:
    print(f"lit(\"The {c.replace('_', ' ')} is \"), configs_df[\"{c}\"], lit(\". \"),")

# COMMAND ----------

tier_4_text_df = configs_df.withColumn("COMBINED_TIER_4_TEXT",
                    concat(
                    lit("The COLLAR TYPE is "), configs_df["COLLAR_TYPE"], lit(". "),
                    lit("The MATERIAL is "), configs_df["MATERIAL"], lit(". ")
                   ))

# COMMAND ----------

tier_4_text_embedding_df = prepare_embedding(tier_4_text_df, spark, col_to_embed='COMBINED_TIER_4_TEXT', dimensions=tier_4_text_dim)
tier_4_text_embedding_df = tier_4_text_embedding_df.select(tier_4_text_embedding_df.INDEX, tier_4_text_embedding_df.embeddings.alias("TIER_4_TEXT_EMBEDDINGS"))
tier_4_text_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 2

# COMMAND ----------

# COLOR & MOQ
tier_2_num_dim = feature_tier_weight['tier_2']

scaler_l = StandardScaler()
scaler_a = StandardScaler()
scaler_b = StandardScaler()
scaler_moq = StandardScaler()


# Assuming you have your data stored in a DataFrame called 'items'
# Normalize numerical features
configs_pd['L_COLOUR_SCALED'] = scaler_l.fit_transform(configs_pd['L_COLOR'].values.reshape(-1, 1))
configs_pd['A_COLOUR_SCALED'] = scaler_a.fit_transform(configs_pd['A_COLOR'].values.reshape(-1, 1))
configs_pd['B_COLOUR_SCALED'] = scaler_b.fit_transform(configs_pd['B_COLOR'].values.reshape(-1, 1))

#items['Size_numerical_SCALED'] = scaler_size.fit_transform(items['Size_numerical'].values.reshape(-1, 1))
configs_pd['MOQ_SCALED'] = scaler_moq.fit_transform(configs_pd['MIN_QUANTITY'].values.reshape(-1, 1))

input_l_colour = Input(shape=(1,), name='input_l')
input_a_colour = Input(shape=(1,), name='input_a')
input_b_colour = Input(shape=(1,), name='input_b')
#input_size_numerical = Input(shape=(1,), name='input_size_numerical')
input_moq = Input(shape=(1,), name='input_moq')



tier_2_num_concatenated_inputs = Concatenate()([input_l_colour, input_a_colour, input_b_colour, input_moq])
tier_2_num_dense_layer = Dense(tier_2_num_dim, activation='relu')(tier_2_num_concatenated_inputs)
tier_2_num_output_layer = Dense(tier_2_num_dim, activation='linear')(tier_2_num_dense_layer)

tier_2_num_model = Model(inputs=[input_l_colour, input_a_colour, input_b_colour, input_moq], outputs=tier_2_num_output_layer)

# Compile the model
tier_2_num_model.compile(optimizer='adam', loss='mse')

# Extract embeddings for your data
tier_2_num_encoder_model = Model(inputs=tier_2_num_model.input, outputs=tier_2_num_model.layers[-2].output)

# model.fit(items.values, labels, epochs=10, batch_size=32)
# Get the embeddings
#embeddings = encoder_model.predict([items['L_COLOUR_SCALED'], items['A_COLOUR_SCALED'], items['B_COLOUR_SCALED'], item['Size_numerical_SCALED'], items['MOQ_SCALED']])
tier_2_num_embeddings = tier_2_num_encoder_model.predict([configs_pd['L_COLOUR_SCALED'], configs_pd['A_COLOUR_SCALED'], configs_pd['B_COLOUR_SCALED'], configs_pd['MOQ_SCALED']])

# Add embeddings to the DataFrame
configs_pd['TIER_2_NUMERICAL_EMBEDDINGS'] = list(tier_2_num_embeddings)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tier 4

# COMMAND ----------

# Back
tier_4_num_dim = feature_tier_weight['tier_4']

scaler_back = StandardScaler()


# Assuming you have your data stored in a DataFrame called 'items'
# Normalize numerical features
configs_pd['BACK'] = scaler_back.fit_transform(configs_pd['BACK'].values.reshape(-1, 1))

input_back = Input(shape=(1,), name='input_back')




tier_4_num_concatenated_inputs = Concatenate()([input_back])
tier_4_num_dense_layer = Dense(tier_4_num_dim, activation='relu')(tier_4_num_concatenated_inputs)
tier_4_num_output_layer = Dense(tier_4_num_dim, activation='linear')(tier_4_num_dense_layer)

tier_4_num_model = Model(inputs=[input_back], outputs=tier_4_num_output_layer)

# Compile the model
tier_4_num_model.compile(optimizer='adam', loss='mse')

# Extract embeddings for your data
tier_4_num_encoder_model = Model(inputs=tier_4_num_model.input, outputs=tier_4_num_model.layers[-2].output)

# model.fit(items.values, labels, epochs=10, batch_size=32)
# Get the embeddings
#embeddings = encoder_model.predict([items['L_COLOUR_SCALED'], items['A_COLOUR_SCALED'], items['B_COLOUR_SCALED'], item['Size_numerical_SCALED'], items['MOQ_SCALED']])
tier_4_num_embeddings = tier_4_num_encoder_model.predict([configs_pd['BACK']])

# Add embeddings to the DataFrame
configs_pd['TIER_4_NUMERICAL_EMBEDDINGS'] = list(tier_4_num_embeddings)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Categorical Embeddings

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

lista = []
lista_inputs = []
for i in ['GENDER','SLEEVE_STYLE','DECORATION_LOCATION_1','DECORATION_TECHNOLOGY_1','COLLAR_TYPE']:
  encoder = LabelEncoder()
  configs_pd[i+'_ENCODED'] = encoder.fit_transform(configs_pd[i])
  num_categories = len(encoder.classes_)
  embedding_dim = 16

  # Define inputs
  input_name = i.lower() + '_input'
  category_input = Input(shape=(1,), name=input_name)
  lista_inputs.append(category_input)
  # Define embedding layers
  input_embedding = i.lower() + '_embedding'
  category_embedding = Embedding(input_dim=num_categories, output_dim=embedding_dim, name=input_embedding)(category_input)

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

categorical_embeddings = model.predict([configs_pd['GENDER_ENCODED'].values, configs_pd['SLEEVE_STYLE_ENCODED'].values, configs_pd['DECORATION_LOCATION_1_ENCODED'].values, configs_pd['DECORATION_TECHNOLOGY_1_ENCODED'].values, configs_pd['COLLAR_TYPE_ENCODED'].values])

configs_pd['CATEGORICAL_EMBEDDINGS'] = list(categorical_embeddings)

# COMMAND ----------

numerical_embedding_df = spark.createDataFrame(configs_pd)
numerical_embedding_df = numerical_embedding_df.select("INDEX","TIER_2_NUMERICAL_EMBEDDINGS","TIER_4_NUMERICAL_EMBEDDINGS","CATEGORICAL_EMBEDDINGS")
numerical_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Similarity Calcs

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join Embeddings

# COMMAND ----------

full_df = (tier_1_text_embedding_df#.join(tier_2_text_embedding_df, ["INDEX"])
              #.join(tier_3_text_embedding_df, ["INDEX"])
              #.join(tier_4_text_embedding_df, ["INDEX"])
              .join(numerical_embedding_df, ["INDEX"]))

# full_df = full_df.withColumn('FULL_EMBEDDING', concat(col('TIER_1_TEXT_EMBEDDINGS'), col('TIER_2_TEXT_EMBEDDINGS'), col('TIER_3_TEXT_EMBEDDINGS'), col('TIER_4_TEXT_EMBEDDINGS'), col('TIER_2_NUMERICAL_EMBEDDINGS'), col('TIER_4_NUMERICAL_EMBEDDINGS')))
full_df = full_df.withColumn('FULL_EMBEDDING', concat(col('TIER_1_TEXT_EMBEDDINGS'), col('TIER_2_NUMERICAL_EMBEDDINGS'), col("CATEGORICAL_EMBEDDINGS")))

full_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pairwise Comps

# COMMAND ----------

embeddings_list = full_df.select(col("FULL_EMBEDDING")).rdd.flatMap(lambda x: x).collect()
index_list = full_df.select(col("INDEX")).rdd.flatMap(lambda x: x).collect()

emb_tensor = torch.tensor(embeddings_list, dtype=torch.float32)
if emb_tensor.ndimension() == 1:
    emb_tensor = emb_tensor.unsqueeze(0)

# COMMAND ----------

cosine_scores = util.cos_sim(emb_tensor, emb_tensor)
score_list = cosine_scores.tolist()

# COMMAND ----------

index_scores_zip_list = []
for i in range(len(index_list)):
    for j in range(len(index_list)):
        index_scores_zip_list.append((index_list[i], (index_list[j], score_list[i][j])))

# COMMAND ----------

pairwise_df = spark.createDataFrame(
    [(index_scores_zip_list[i][0], index_scores_zip_list[i][1][0], index_scores_zip_list[i][1][1]) for i in range(len(index_scores_zip_list))],
    ["SOURCE_INDEX", "COMPARISON_INDEX", "SIMILARITY_SCORE"]
)

pairwise_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top K

# COMMAND ----------

def topk_similar(similarity_df, feature_df, prd_index_list, k=5):

    source_df = feature_df.join(similarity_df, feature_df.INDEX == similarity_df.SOURCE_INDEX).select('SOURCE_INDEX','PRODUCT_KEY', 'COMPARISON_INDEX', 'SIMILARITY_SCORE').withColumnRenamed("PRODUCT_KEY", "SOURCE_PRODUCT_KEY")

    comparison_df = feature_df.join(similarity_df, feature_df.INDEX == similarity_df.COMPARISON_INDEX).select('SOURCE_INDEX','COMPARISON_INDEX','PRODUCT_KEY').withColumnRenamed("PRODUCT_KEY", "COMPARISON_PRODUCT_KEY")

    pairwise_product_df = source_df.join(comparison_df, (source_df.SOURCE_INDEX == comparison_df.SOURCE_INDEX) & (source_df.COMPARISON_INDEX == comparison_df.COMPARISON_INDEX)).select(source_df.SOURCE_INDEX, "SOURCE_PRODUCT_KEY", source_df.COMPARISON_INDEX, "COMPARISON_PRODUCT_KEY", "SIMILARITY_SCORE")

    unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_PRODUCT_KEY != pairwise_product_df.COMPARISON_PRODUCT_KEY)

    #comparison_prod_ranking = Window.partitionBy("SOURCE_INDEX","COMPARISON_PRODUCT_KEY").orderBy(desc("SIMILARITY_SCORE"), "COMPARISON_INDEX")
    sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("SIMILARITY_SCORE"), "COMPARISON_INDEX")

    #ranked_sim_df = unranked_sim_df.withColumn("product_rank", rank().over(comparison_prod_ranking))
    #ranked_sim_df = ranked_sim_df.where(ranked_sim_df.product_rank == 1)
    ranked_final_df = unranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking)).select('SOURCE_INDEX','SOURCE_PRODUCT_KEY', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'SIMILARITY_SCORE', 'FINAL_SIMILARITY_RANK')

    output = ranked_final_df.where((ranked_final_df.SOURCE_INDEX.isin(prd_index_list)) & (ranked_final_df.FINAL_SIMILARITY_RANK <= k)).orderBy("SOURCE_INDEX","FINAL_SIMILARITY_RANK")
    return(output)

# COMMAND ----------

lookup_items = ['a044f599675d887c855e43cb3013bf80fdf5f40e7d81af57e0632bdbb19b0c0c','b5f97bb0b15e824ff8ee4b0f7dc069708e689358e873e262d20c3066ab893b2b','c7e8b7e1e8b8b0d2834b44667352b008ea8d04d6d5b7483e2731d759886c953d']

topk_example = topk_similar(pairwise_df, configs_df, lookup_items, 5)
topk_example.display()

# COMMAND ----------

def topk_manual_review(topk_df, feature_df):

    columns_to_compare = ['PRODUCT_KEY', 'CATEGORY', 'SUBCATEGORY', 'PRODUCT_GROUP', 'PRODUCT_NAME', 'GENDER', 'SLEEVE_STYLE', 'COLOUR', 'HUMAN_COLOR', 'BRAND_NAME', 'COLLAR_TYPE', 'DECORATION_LOCATION_1', 'DECORATION_TECHNOLOGY_1', 'BACK', 'MATERIAL', 'MIN_QUANTITY']

    topk_source_attributes =  feature_df.join(topk_df, feature_df.INDEX == topk_df.SOURCE_INDEX)
    topk_comparison_attributes =  feature_df.join(topk_df, feature_df.INDEX == topk_df.COMPARISON_INDEX)

    num_cols = len(columns_to_compare)
    expr_str = "stack(" + str(num_cols) + ", " + ", ".join([f"'{col}', CAST({col} AS STRING)" for col in columns_to_compare]) + ") as (ATTRIBUTE, VALUE)"

    source_melted_df = topk_source_attributes.select("SOURCE_INDEX","SOURCE_PRODUCT_KEY",'COMPARISON_INDEX', "COMPARISON_PRODUCT_KEY", "SIMILARITY_SCORE", "FINAL_SIMILARITY_RANK", expr(expr_str)).withColumnRenamed("VALUE", "SOURCE_ATTRIBUTE_VALUE")
    comparision_melted_df = topk_comparison_attributes.select("SOURCE_INDEX",'COMPARISON_INDEX', expr(expr_str)).withColumnRenamed("VALUE", "COMPARISON_ATTRIBUTE_VALUE")

    full_melted_df = source_melted_df.join(comparision_melted_df, (source_melted_df.SOURCE_INDEX == comparision_melted_df.SOURCE_INDEX) & \
                                            (source_melted_df.COMPARISON_INDEX == comparision_melted_df.COMPARISON_INDEX) & \
                                            (source_melted_df.ATTRIBUTE == comparision_melted_df.ATTRIBUTE)).select(source_melted_df.SOURCE_INDEX, source_melted_df.SOURCE_PRODUCT_KEY, source_melted_df.COMPARISON_INDEX, source_melted_df.COMPARISON_PRODUCT_KEY, source_melted_df.SIMILARITY_SCORE, source_melted_df.FINAL_SIMILARITY_RANK, source_melted_df.ATTRIBUTE, source_melted_df.SOURCE_ATTRIBUTE_VALUE, comparision_melted_df.COMPARISON_ATTRIBUTE_VALUE) 
    
    return(full_melted_df)

# COMMAND ----------

topk_example_review = topk_manual_review(topk_example, configs_df)
topk_example_review.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out example

# COMMAND ----------

source_df = configs_df.join(pairwise_df, configs_df.INDEX == pairwise_df.SOURCE_INDEX).select('SOURCE_INDEX','PRODUCT_KEY', 'COMPARISON_INDEX', 'SIMILARITY_SCORE').withColumnRenamed("PRODUCT_KEY", "SOURCE_PRODUCT_KEY")

comparison_df = configs_df.join(pairwise_df, configs_df.INDEX == pairwise_df.COMPARISON_INDEX).select('SOURCE_INDEX','COMPARISON_INDEX','PRODUCT_KEY').withColumnRenamed("PRODUCT_KEY", "COMPARISON_PRODUCT_KEY")

pairwise_product_df = source_df.join(comparison_df, (source_df.SOURCE_INDEX == comparison_df.SOURCE_INDEX) & (source_df.COMPARISON_INDEX == comparison_df.COMPARISON_INDEX)).select(source_df.SOURCE_INDEX, "SOURCE_PRODUCT_KEY", source_df.COMPARISON_INDEX, "COMPARISON_PRODUCT_KEY", "SIMILARITY_SCORE")

unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_PRODUCT_KEY != pairwise_product_df.COMPARISON_PRODUCT_KEY)

comparison_prod_ranking = Window.partitionBy("SOURCE_INDEX","COMPARISON_PRODUCT_KEY").orderBy(desc("SIMILARITY_SCORE"), "COMPARISON_INDEX")
sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("SIMILARITY_SCORE"), "COMPARISON_INDEX")

ranked_sim_df = unranked_sim_df.withColumn("product_rank", rank().over(comparison_prod_ranking))
ranked_sim_df = ranked_sim_df.where(ranked_sim_df.product_rank == 1)
ranked_final_df = ranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking)).select('SOURCE_INDEX','SOURCE_PRODUCT_KEY', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'SIMILARITY_SCORE', 'FINAL_SIMILARITY_RANK')

write_out_example = ranked_final_df.join(items_df, ranked_final_df.SOURCE_INDEX == items_df.INDEX).orderBy("SOURCE_INDEX","FINAL_SIMILARITY_RANK","SIZE")

# COMMAND ----------

write_out_example.where(ranked_final_df.SOURCE_INDEX == '0280a36d644dc118b1ffe4fb21298d9a2ffe4c904d07ffff9a0da0f2ce99dfa3').display()

# COMMAND ----------


