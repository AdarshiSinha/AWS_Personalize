# Databricks notebook source
# MAGIC %md
# MAGIC ### Config

# COMMAND ----------

# MAGIC %run ../setups/install_packages

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

from pyspark.ml.functions import array_to_vector, vector_to_array
import webcolors
from pyspark.sql.functions import concat, lit, col, length
from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, MapType, DoubleType,StringType, FloatType
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, round, when, lit, pow, current_timestamp, log
from pyspark.sql.window import Window
from angle_emb import AnglE, Prompts
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from pyspark.sql.types import *
from angle_emb import AnglE, Prompts
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from colormath.color_objects import LabColor, sRGBColor
from colormath.color_conversions import convert_color
from colormath.color_diff import delta_e_cmc
import tensorflow as tf
import torch
import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay
from sentence_transformers import SentenceTransformer, util
from tensorflow.keras.layers import TextVectorization, Embedding, Input, Flatten, Concatenate
from tensorflow.keras.models import Model
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Concatenate
from sklearn.preprocessing import StandardScaler
import boto3 
import pandas as pd
import io
import matplotlib.colors as mcolors
from skimage.color import rgb2lab
import webcolors
from sklearn.preprocessing import LabelEncoder
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# COMMAND ----------

aws_role_session = "dna_ppp_temp_session_gravity"
aws_peresonalize_execution_role = 'arn:aws:iam::905666450942:role/service-role/AmazonPersonalize-ExecutionRole-1595956851346'
aws_personalize_role = dbutils.secrets.get(scope="personalizationProductRecommender", key="awsPersonalizeAccountRole")
aws_account_id = "905666450942"

# COMMAND ----------

client = boto3.client('sts')
StsResponse = client.assume_role(RoleArn=aws_personalize_role, RoleSessionName=aws_role_session)
s3_client =boto3.client('s3',
                     aws_access_key_id=StsResponse['Credentials']['AccessKeyId'],
                        aws_secret_access_key=StsResponse['Credentials']['SecretAccessKey'],
                        aws_session_token=StsResponse['Credentials']['SessionToken'])
#from recommender.aws_service.s3_client import S3Client
S3_BUCKET = "precs-product-recommendation-dev"

# COMMAND ----------

import io
latest_users_path = 'config/configurations.csv'
obj = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_users_path)
items = pd.read_csv(io.BytesIO(obj['Body'].read()))
items = items.drop(columns= 'SIZE')
items = items.drop_duplicates().reset_index(drop=True)

# COMMAND ----------

items

# COMMAND ----------

#df = spark.createDataFrame(items.sample(100, random_state=10))
df = spark.createDataFrame(items)
df = df.where(length(col("COLOUR")) == 7)
df.display()

# COMMAND ----------

def hex_to_lab(hex_code):
    r_c, g_c, b_c  = webcolors.hex_to_rgb(hex_code)
    rgb = sRGBColor(r_c / 255, g_c/ 255, b_c/ 255)
    lab = convert_color(rgb, LabColor).get_value_tuple()
    return(lab)

# COMMAND ----------

rand_blue = hex_to_lab("#3C62AD")
print(rand_blue)
another_blue = hex_to_lab("#335eac")
print(another_blue)
pinkish = hex_to_lab("#fd4d7c")
print(pinkish)

# COMMAND ----------

import numpy

def patch_asscalar(a):
    return a.item()

setattr(numpy, "asscalar", patch_asscalar)

# COMMAND ----------

# delta_e_cmc(rand_blue, another_blue, pl=2, pc=1)

# COMMAND ----------

# delta_e_cmc(rand_blue, pinkish, pl=2, pc=1)

# COMMAND ----------

prod_config_query = f"""
with product_active_versions as (
    select
        ps.productid as product_key
        , ps.productversion as product_version
    FROM mcp.merchant_prod.vw_product_status ps
    where upper(ps.status) in ('OPEN', 'ACTIVE')
),

configs as (
select pc.product_key, pc.product_name, pc.category, pc.subcategory, pc.product_group, pe.COUNTRY_CODE, pe.FULFILLMENT_OPTIONS,
d.IS_CURRENT, d.BRAND_NAME, d.COLLAR_TYPE, d.DECORATION_LOCATION_1, d.BACK, d.DECORATION_TECHNOLOGY_1, d.GENDER, d.MATERIAL,
d.MIN_QUANTITY, d.SLEEVE_STYLE
from SANDBOX.DNA_PRODUCT_DEV.FULFILLMENT_OPTIONS_CONFIG pe
join vistaprint.product.product_categorization pc
on pe.product_key = pc.product_key
left join
(select PRODUCT_KEY, IS_CURRENT,
TRIM(GET(BRAND_NAME,0),'"') AS BRAND_NAME,
TRIM(GET(COLLAR_TYPE,0),'"') AS COLLAR_TYPE,
TRIM(GET(DECORATION_LOCATION_1,0),'"') AS DECORATION_LOCATION_1,
IFF(TRIM(GET(DECORATION_LOCATION_2,0),'"') = 'BACK', 1, 0) AS BACK,
TRIM(GET(DECORATION_TECHNOLOGY_1,0),'"') AS DECORATION_TECHNOLOGY_1,
TRIM(GET(GENDER,0),'"') AS GENDER,
TRIM(GET(MATERIAL,0),'"') AS MATERIAL,
MIN_QUANTITY,
TRIM(GET(SLEEVE_STYLE,0),'"') AS SLEEVE_STYLE
from vistaprint.product.mcp_sku_product_config) AS d
ON d.PRODUCT_KEY = pc.PRODUCT_KEY
where pc.product_group in ('T-shirts','Polos')
and (right(pc.product_key,1) = 'W' OR right(pc.product_key,1) = 'R')
and pe.country_code = 'US'
and d.IS_CURRENT = TRUE)
,

configs_flattened as (
SELECT
DISTINCT 
    product_key, product_name, category, subcategory, product_group,
    BRAND_NAME, COLLAR_TYPE, DECORATION_LOCATION_1, BACK,
    DECORATION_TECHNOLOGY_1, GENDER, MATERIAL
    --, MIN_QUANTITY
    , SLEEVE_STYLE,
    --trim(FULFILLMENT_OPTIONS:Size, '"') AS Size,
    CASE
        WHEN TRIM(FULFILLMENT_OPTIONS:"Substrate Color", '"') IS NOT NULL THEN TRIM(FULFILLMENT_OPTIONS:"Substrate Color", '"')
        WHEN TRIM(FULFILLMENT_OPTIONS:"Substrate color", '"') IS NOT NULL THEN TRIM(FULFILLMENT_OPTIONS:"Substrate color", '"')
        WHEN TRIM(FULFILLMENT_OPTIONS:"Color", '"') IS NOT NULL THEN TRIM(FULFILLMENT_OPTIONS:"Color", '"')
        ELSE NULL
    END AS COLOUR

FROM
    configs)

select *
from configs_flattened
where len(COLOUR) = 7
"""

# COMMAND ----------

# prod_config_query = f"""
# with product_active_versions as (
#     select 
#         ps.productid as product_key
#         , ps.productversion as product_version
#     FROM mcp.merchant_prod.vw_product_status ps
#     where upper(ps.status) in ('OPEN', 'ACTIVE')
# )
# , product_available_countries as (
#     select distinct
#         pscs.product_id as product_key
#         , pscs.product_version as current_product_version
#         , pscs.contexts:Country::varchar as country_code
#         ,pscs.iscurrent
#     from mcp.merchant_prod.vw_product_status_context_secured pscs
#     where upper(pscs.contexts:Merchant::varchar) = 'VISTAPRINT' 
#     and upper(pscs.status) in ('OPEN', 'ACTIVE') 
#     and country_code is not null
#     and pscs.iscurrent = 'true')

    
# select distinct tm.product_key, pac.country_code, tm.category, tm.subcategory, tm.product_group, tm.product_name, tm.substrate_color
# --, tm.gender, tm.deco_tech as decoration_technology, tm.deco_area as decoration_area, tm.deco_location as decoration_location, tm.brand, tm.brand_name, tm.material
# from vistaprint.product.fulfillment_options_tabular_mapping tm
# join product_available_countries pac
# on tm.product_key = pac.product_key
# and tm.countrycode = pac.country_code
# where tm.product_group in ('T-shirts','Polos')
# and right(tm.product_key,1) = 'W'
# and pac.country_code = 'US'
# and len(tm.substrate_color) = 7
# """

# COMMAND ----------

#df = snowflake.execute_reader(prod_config_query)
df = df.withColumn("INDEX", f.sha2(f.concat(*(f.col(c).cast("string") for c in ["PRODUCT_KEY", "COLOUR"])), 256))

# COMMAND ----------


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

# COMMAND ----------

from pyspark.sql.types import ArrayType, StringType

my_func_udf = udf(get_colour_name, StringType())

# COMMAND ----------

df = df.withColumn("COLOR", my_func_udf(df["COLOUR"])).drop("COLOUR")
df.display()

# COMMAND ----------

# df.select('COLOUR').distinct().display()

# COMMAND ----------

focus_cols = [c.lower() for c in df.columns if c not in ['INDEX','PRODUCT_KEY', 'COUNTRY_CODE','CATEGORY','PRODUCT_NAME']]
focus_cols

# COMMAND ----------

#logic for concat
for c in focus_cols:
    print(f"lit(\"The {c.replace('_', ' ')} is \"), df[\"{c}\"], lit(\". \"),")

# COMMAND ----------


df = df.na.fill('')
df2 = df.withColumn("COMBINED_TEXT",
                    concat(
                        lit("The subcategory is "), df["subcategory"], lit(". "),
                        lit("The product group is "), df["product_group"], lit(". "),
                        lit("The brand name is "), df["brand_name"], lit(". "),
                        lit("The collar type is "), df["collar_type"], lit(". "),
                        lit("The decoration location 1 is "), df["decoration_location_1"], lit(". "),
                        lit("The back is "), df["back"], lit(". "),
                        lit("The decoration technology 1 is "), df["decoration_technology_1"], lit(". "),
                        lit("The gender is "), df["gender"], lit(". "),
                        lit("The sex is "), df["gender"], lit(". "),
                        lit("The identity is "), df["gender"], lit(". "),
                        lit("The style is "), df["gender"], lit(". "),
                        lit("The material is "), df["material"], lit(". "),
                        lit("The min quantity is "), df["min_quantity"], lit(". "),
                        lit("The sleeve style is "), df["sleeve_style"], lit(". "),
                        lit("The color is "), df["color"], lit(". "),
                        lit("The hue is "), df["color"], lit(". "),
                        lit("The shade is "), df["color"], lit(". ")
                   ))

# COMMAND ----------

df2.display()

# COMMAND ----------

def prepare_embedding(text_df,spark,culture = "en"):
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
        text_array = text_df.select("COMBINED_TEXT").rdd.flatMap(lambda x: x).collect()
        
        # angle = AnglE.from_pretrained('WhereIsAI/UAE-Large-V1', pooling_strategy='cls').cuda()
        # pipe = pipeline(model="WhereIsAI/UAE-Large-V1",pooling_strategy='cls',device=0)
        # text_embedding = pipe(text_array,batch_size=64)
        # text_embedding = angle.encode(text_array,batch_size=4)
        angle = SentenceTransformer('WhereIsAI/UAE-Large-V1').cpu()
        #angle = SentenceTransformer('WhereIsAI/UAE-Large-V1').cuda()
        text_embedding = angle.encode(text_array,batch_size=64)


        
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

embedding_df = prepare_embedding(df2,spark)

# COMMAND ----------

embedding_df = embedding_df.withColumn("PRODUCT_LABEL",concat(col('COLOR'), col('PRODUCT_NAME')))
embedding_df.display()

# COMMAND ----------

embeddings_list = embedding_df.select(col("embeddings")).rdd.flatMap(lambda x: x).collect()
prod_labels = embedding_df.select(col("PRODUCT_LABEL")).rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

emb_tensor = torch.tensor(embeddings_list, dtype=torch.float32)

# COMMAND ----------

if emb_tensor.ndimension() == 1:
    emb_tensor = emb_tensor.unsqueeze(0)

# COMMAND ----------

df_cosine_scores = util.cos_sim(emb_tensor, emb_tensor)

# COMMAND ----------

# disp = ConfusionMatrixDisplay(confusion_matrix=df_cosine_scores.numpy(), display_labels=prod_labels)
# fig, ax = plt.subplots(figsize=(26,26))
# disp.plot(include_values=False, xticks_rotation = 'vertical', cmap='Blues', ax = ax) 
# plt.show()

# COMMAND ----------

lookup_df = embedding_df.withColumn("row_id", monotonically_increasing_id())
lookup_df.display()

# COMMAND ----------

def most_similar_products(product_index, df, cos_scores, n_products):
    prod_to_compare_id = df.filter(df["INDEX"] == product_index).select("row_id").first()[0]
    prod_full_similarities = cos_scores[prod_to_compare_id]

    topk_similar_prods = torch.topk(prod_full_similarities.flatten(), n_products).indices.numpy().tolist()
    topk_similar_scores = prod_full_similarities[topk_similar_prods].numpy().tolist()
    similar_data_zipped = list(zip(topk_similar_prods, topk_similar_scores))

    scored_df = spark.createDataFrame(similar_data_zipped, ['row_id','cosine_similarity']) 
    filtered_df = df.filter(df["row_id"].isin(topk_similar_prods))
    output_df = filtered_df.join(scored_df, on='row_id').sort("cosine_similarity",ascending = [False]) 
    
    return(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Gray Mens Long sleeve

# COMMAND ----------

most_similar_products(product_index= "ec8da683a5f758da492c9472a4fce38816c0e017b5721c0603399a6af2a42de8"
                      , df= lookup_df
                      , cos_scores= df_cosine_scores
                      , n_products = 15).display()

# COMMAND ----------

# MAGIC %md Green Womens short sleeve
# MAGIC

# COMMAND ----------

most_similar_products(product_index= "4b6a970385e8e231b1416baa004d5e940f8e5cc157c6d84ebc085a3c137030a5"
                      , df= lookup_df
                      , cos_scores= df_cosine_scores
                      , n_products = 15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Black womens polo with pocket

# COMMAND ----------

most_similar_products(product_index= "1cbeb7a18db92521d5e813d9ef3e4a3425bfc443f5a58c26ae185e9b9eb7ab00"
                      , df= lookup_df
                      , cos_scores= df_cosine_scores
                      , n_products = 15).display()

# COMMAND ----------


