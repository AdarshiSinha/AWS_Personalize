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
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, asc, round, when, lit, pow, current_timestamp, log, monotonically_increasing_id, row_number, concat, array, struct, rank, dense_rank,  from_json, to_json, collect_list, transform
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
def patch_asscalar(a):
    return a.item()
setattr(np, "asscalar", patch_asscalar)
import pandas as pd
import pyspark.sql.functions as f
import sys
import torch
import webcolors
from colormath.color_objects import LabColor, sRGBColor
from colormath.color_conversions import convert_color
from scipy.sparse import coo_matrix
import json


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
    return convert_color(rgb_color, LabColor)

def hex_to_lab(hex_color):
    rgb_color = sRGBColor.new_from_rgb_hex(hex_color)
    lab_color = convert_color(rgb_color, LabColor)
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
        text_embedding = model.encode(text_array,batch_size=64)


        
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

items_query = f"""
WITH standard_product_configs AS (
    SELECT
        m.PRODUCT_KEY,
        PRODUCT_VERSION,
        TRIM(GET(BRAND_NAME, 0), '"') AS BRAND_NAME,
        TRIM(GET(COLLAR_TYPE, 0), '"') AS COLLAR_TYPE,
        TRIM(GET(DECORATION_LOCATION_1, 0), '"') AS DECORATION_LOCATION_1,
        --GET(DECORATION_LOCATION_2, 0) AS DECORATION_LOCATION_2,
        TRIM(GET(DECORATION_TECHNOLOGY_1, 0), '"') AS DECORATION_TECHNOLOGY_1,
        TRIM(GET(GENDER, 0), '"') AS GENDER,
        TRIM(GET(MATERIAL, 0), '"') AS MATERIAL,
        --MIN_QUANTITY,
        TRIM(GET(SLEEVE_STYLE, 0), '"') AS SLEEVE_STYLE
    FROM
        VISTAPRINT.PRODUCT.MCP_SKU_PRODUCT_CONFIG m
        join vistaprint.product.product_categorization pc on m.product_key = pc.product_key
    WHERE
        is_current = TRUE
        and pc.product_group in ('T-shirts', 'Polos')
        --and pc.product_key = 'PRD-J2VM1PY8B'
),
flattened_pricing as (
    select
        MERCHANTPRODUCTID as product_key,
        merchantproductversion as product_version,
        JSON :userSelections as user_selections,
        array_min(JSON :quantities) as quantities_min,
        prices.VALUE :market :: string as Market,
        prices.VALUE :currency :: string as Currency,
        prices.VALUE :prices as list_prices,
        JSON :updatedAt :: timestamp_tz as RECORD_UDPATE_STAMP
    from
        mcp.pricing.price_enumerations_all as e
        join standard_product_configs spc on e.merchantproductid = spc.product_key
        and e.merchantproductversion = spc.product_version,
        LATERAL FLATTEN(OUTER => TRUE, INPUT => e."JSON" :"listPrices") prices
    where
        prices.VALUE :merchantId = 'VISTAPRINT'
        and Market = 'US' 
    qualify row_number() over(partition by MerchantProductId, Market, user_selections order by RECORD_UDPATE_STAMP desc) = 1
),
priced_quantities as (
    SELECT
        product_key,
        user_selections,
        Market,
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
        spc.brand_name,
        spc.collar_type,
        spc.material,
        spc.sleeve_style,
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
        coalesce(
            TRIM(em.user_selections: "Substrate Color", '"'),
            TRIM(em.user_selections: "substrate color", '"')
        ) AS COLOUR,
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
    pc.product_name,
    pc.category,
    pc.subcategory,
    pc.product_group,
    p.BRAND_NAME,
    p.COLLAR_TYPE,
    p.DECORATION_LOCATION_1,
    p.Backside,
    p.DECORATION_TECHNOLOGY_1,
    p.GENDER,
    p.MATERIAL,
    p.MIN_QUANTITY,
    p.SLEEVE_STYLE,
    p.Size,
    p.COLOUR,
    p.moq_unit_price,
    p.user_selections
FROM
    priced_configs p
    join VISTAPRINT.PRODUCT.PRODUCT_CATEGORIZATION pc ON p.product_key = pc.product_key
where  len(p.COLOUR) = 7
    and left(p.COLOUR,1) = '#'

"""
#,'G','D','I','K'
#    --and right(product_key,1) in ('W','Z','T','V','C','R','L','A') 
# --pc.product_key in ('PRD-SJSSJY2V6', 'PRD-UHKXEZ2LR')

# COMMAND ----------

product_options_schema = StructType([
    StructField("key", StringType(), False),
    StructField("version", IntegerType(), False),
    StructField("options", MapType(StringType(), StringType()), False)
])

selections_schema = MapType(StringType(), StringType())


# COMMAND ----------

initial_data = snowflake.execute_reader(items_query)

initial_data = initial_data.withColumn("PRODUCT_VERSION", initial_data.PRODUCT_VERSION.cast('integer'))
initial_data = initial_data.withColumn("MIN_QUANTITY", initial_data.MIN_QUANTITY.cast('integer'))
initial_data = initial_data.withColumn("USER_SELECTIONS", from_json(col("USER_SELECTIONS"), selections_schema))
initial_data = initial_data.withColumn("USER_SELECTIONS_CONFIG_ONLY", expr("map_filter(USER_SELECTIONS, (k, v) -> k != 'Size')"))
#initial_data = initial_data.drop(*['USER_SELECTIONS','USER_SELECTIONS_CONFIG_ONLY'])

missing_values_cleaning = {'BACKSIDE':'Blank'}
initial_data = initial_data.na.fill(missing_values_cleaning)

items = initial_data.toPandas()

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
items[['L_COLOR', 'A_COLOR', 'B_COLOR']] = items['COLOUR'].apply(lambda x: pd.Series(hex_to_lab(x).get_value_tuple()))

# COMMAND ----------

items[['HUMAN_COLOR']] = items['COLOUR'].apply(lambda x: pd.Series(get_colour_name(x)))

# COMMAND ----------

items_df = spark.createDataFrame(items)
items_df = items_df.na.fill('')
items_df = items_df.withColumn("INDEX", f.sha2(f.concat(*(f.col(c).cast("string") for c in ["PRODUCT_KEY", "MARKET","USER_SELECTIONS_CONFIG_ONLY"])), 256)).cache()
items_pd = items_df.toPandas()

items_df = items_df.withColumn("USER_SELECTIONS_CONFIG_ONLY_STR", to_json(col("USER_SELECTIONS_CONFIG_ONLY")))
configs_df = items_df.dropDuplicates(["INDEX", "USER_SELECTIONS_CONFIG_ONLY_STR"])
configs_df = configs_df.withColumn("USER_SELECTIONS_CONFIG_ONLY", from_json(col("USER_SELECTIONS_CONFIG_ONLY_STR"), selections_schema))
configs_df = configs_df.drop("USER_SELECTIONS_CONFIG_ONLY_STR")
configs_pd = configs_df.toPandas()

# COMMAND ----------

# user selected options only (no size)
configs_df.count()

# COMMAND ----------

# user selected options only (includes size)
items_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text Embeddings

# COMMAND ----------

text_df = configs_df.withColumn("COMBINED_TEXT",
                    # Tier 1 (8)
                    concat(lit("The category is "), configs_df["SUBCATEGORY"], lit(". "),
                        lit("The product group is "), configs_df["PRODUCT_GROUP"], lit(". "),
                        lit("The "), configs_df["PRODUCT_GROUP"], lit(" is intended for "), configs_df["GENDER"], lit(". "),
                        lit("The sleeve style is "), configs_df["SLEEVE_STYLE"], lit(". "),
                         lit("The decoration area is "), configs_df["DECORATION_LOCATION_1"], lit(". "),
                         lit("The decoration style is "), configs_df["DECORATION_TECHNOLOGY_1"], lit(". "),
                         lit("The material is "), configs_df["MATERIAL"], lit(". "),
                         lit("The collar style is "), configs_df["COLLAR_TYPE"], lit(". ")
                        
                   ))

# COMMAND ----------

text_embedding_df = prepare_embedding(text_df, spark, col_to_embed='COMBINED_TEXT', dimensions=7
                                             )
text_embedding_df = text_embedding_df.withColumnRenamed('embeddings', 'TEXT_EMBEDDINGS')
text_embedding_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical Embeddings

# COMMAND ----------

# scaler_moq = StandardScaler()
scaler_price = StandardScaler()
scaler_l = StandardScaler()
scaler_a = StandardScaler()
scaler_b = StandardScaler()


# Assuming you have your data stored in a DataFrame called 'items'
# Normalize numerical features
# configs_pd['MOQ_SCALED'] = scaler_moq.fit_transform(configs_pd['MIN_QUANTITY'].values.reshape(-1, 1))

configs_pd['MOQ_UNIT_PRICE_SCALED'] = scaler_price.fit_transform(configs_pd['MOQ_UNIT_PRICE'].values.reshape(-1, 1))
configs_pd['L_COLOUR_SCALED'] = scaler_l.fit_transform(configs_pd['L_COLOR'].values.reshape(-1, 1))
configs_pd['A_COLOUR_SCALED'] = scaler_a.fit_transform(configs_pd['A_COLOR'].values.reshape(-1, 1))
configs_pd['B_COLOUR_SCALED'] = scaler_b.fit_transform(configs_pd['B_COLOR'].values.reshape(-1, 1))

#input_moq = Input(shape=(1,), name='input_moq')
input_price = Input(shape=(1,), name='input_moq')
input_l_colour = Input(shape=(1,), name='input_l')
input_a_colour = Input(shape=(1,), name='input_a')
input_b_colour = Input(shape=(1,), name='input_b')

num_concatenated_inputs = Concatenate()([input_price, input_l_colour, input_a_colour, input_b_colour])
num_dense_layer = Dense(3, activation='relu')(num_concatenated_inputs)
num_output_layer = Dense(3, activation='linear')(num_dense_layer)

num_model = Model(inputs=[input_price,input_l_colour, input_a_colour, input_b_colour], outputs= num_output_layer)

# Compile the model
num_model.compile(optimizer='adam', loss='mse')

# Extract embeddings for your data
num_encoder_model = Model(inputs=num_model.input, outputs=num_model.layers[-2].output)

num_embeddings = num_encoder_model.predict([configs_pd['MOQ_UNIT_PRICE_SCALED'], configs_pd['L_COLOUR_SCALED'], configs_pd['A_COLOUR_SCALED'], configs_pd['B_COLOUR_SCALED']])

# Add embeddings to the DataFrame
configs_pd['NUMERICAL_EMBEDDINGS'] = list(num_embeddings)
configs_pd['NUMERICAL_EMBEDDINGS'] = configs_pd['NUMERICAL_EMBEDDINGS'].apply(lambda x: x.tolist())
configs_pd['NUMERICAL_EMBEDDINGS'] = configs_pd['NUMERICAL_EMBEDDINGS'].apply(json.dumps)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Categorical Embeddings

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

lista = []
lista_inputs = []
for i in ['BACKSIDE']: #,'DECORATION_TECHNOLOGY_1','COLLAR_TYPE']:#'GENDER','SLEEVE_STYLE','DECORATION_LOCATION_1','DECORATION_TECHNOLOGY_1','COLLAR_TYPE']:
  encoder = LabelEncoder()
  configs_pd[i+'_ENCODED'] = encoder.fit_transform(configs_pd[i])
  num_categories = len(encoder.classes_)
  embedding_dim = 1

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

categorical_embeddings = model.predict([#configs_pd['GENDER_ENCODED'].values
                                        #, 
                                        configs_pd['BACKSIDE_ENCODED'].values
                                        #, 
                                        #configs_pd['DECORATION_LOCATION_1_ENCODED'].values, configs_pd['DECORATION_TECHNOLOGY_1_ENCODED'].values, configs_pd['SLEEVE_STYLE_ENCODED'].values
                                        ])

configs_pd['CATEGORICAL_EMBEDDINGS'] = list(categorical_embeddings)
configs_pd['CATEGORICAL_EMBEDDINGS'] = configs_pd['CATEGORICAL_EMBEDDINGS'].apply(lambda x: x.tolist())
configs_pd['CATEGORICAL_EMBEDDINGS'] = configs_pd['CATEGORICAL_EMBEDDINGS'].apply(json.dumps)

# COMMAND ----------

numerical_embedding_df = spark.createDataFrame(configs_pd)

numerical_embedding_df = numerical_embedding_df.select("INDEX", "NUMERICAL_EMBEDDINGS", "CATEGORICAL_EMBEDDINGS")
numerical_embedding_df.display()

# COMMAND ----------

numerical_embedding_df = spark.createDataFrame(configs_pd)
numerical_embedding_df = numerical_embedding_df.select("INDEX","NUMERICAL_EMBEDDINGS","CATEGORICAL_EMBEDDINGS")
array_schema = ArrayType(FloatType())
numerical_embedding_df = numerical_embedding_df.withColumn("NUMERICAL_EMBEDDINGS", from_json(col("NUMERICAL_EMBEDDINGS"), array_schema))
numerical_embedding_df = numerical_embedding_df.withColumn("CATEGORICAL_EMBEDDINGS", from_json(col("CATEGORICAL_EMBEDDINGS"), array_schema))
numerical_embedding_df.display()                                                     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Similarity Calcs

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join Embeddings

# COMMAND ----------

full_df = (text_embedding_df.join(numerical_embedding_df, ["INDEX"]))


full_df = full_df.withColumn('FULL_EMBEDDING', concat(col('TEXT_EMBEDDINGS')
                                                      , col('NUMERICAL_EMBEDDINGS')
                                                      , col("CATEGORICAL_EMBEDDINGS")
                             ))

full_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pairwise Comps

# COMMAND ----------

embeddings_array = np.array(full_df.select(col("FULL_EMBEDDING")).rdd.flatMap(lambda x: x).collect())
index_array = np.array(full_df.select(col("INDEX")).rdd.flatMap(lambda x: x).collect()).reshape(-1)

emb_tensor = torch.tensor(embeddings_array, dtype=torch.float32)
if emb_tensor.ndimension() == 1:
    emb_tensor = emb_tensor.unsqueeze(0)

# COMMAND ----------

cosine_scores = util.cos_sim(emb_tensor, emb_tensor)
scores_array_sparse = coo_matrix(cosine_scores.numpy().astype("float"))

# COMMAND ----------

sc = spark.sparkContext.getOrCreate()

comparison_schema = StructType([
    StructField("SOURCE_INDEX", StringType(), False),
    StructField("COMPARISON_INDEX", StringType(), False),
    StructField("PRODUCT_SIMILARITY", FloatType(), False)
])

comparison_rdd = sc.parallelize(range(scores_array_sparse.nnz)).map(
    lambda idx: (
        str(index_array[scores_array_sparse.row[idx]]), 
        str(index_array[scores_array_sparse.col[idx]]), 
        float(scores_array_sparse.data[idx])
    )
)


pairwise_df = spark.createDataFrame(comparison_rdd, comparison_schema)
pairwise_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demo examples

# COMMAND ----------

demo_prd_keys = ['PRD-N7VURKY0W']

demo_unique_index = configs_df.where(configs_df.PRODUCT_KEY.isin(demo_prd_keys)).select("INDEX").rdd.flatMap(lambda x: x).collect()
print(demo_unique_index)
print(len(demo_unique_index))

# COMMAND ----------

demo_df = pairwise_df.where((pairwise_df.SOURCE_INDEX.isin(demo_unique_index))).cache()

# COMMAND ----------

demo_df.display()

# COMMAND ----------

configs_df = configs_df.repartition("INDEX")
demo_df = demo_df.repartition("SOURCE_INDEX", "COMPARISON_INDEX")

# COMMAND ----------

source_df = configs_df.join(demo_df, configs_df.INDEX == demo_df.SOURCE_INDEX).select('SOURCE_INDEX','PRODUCT_KEY', 'COMPARISON_INDEX', 'PRODUCT_SIMILARITY').withColumnRenamed("PRODUCT_KEY", "SOURCE_PRODUCT_KEY")

comparison_df = configs_df.join(demo_df, configs_df.INDEX == demo_df.COMPARISON_INDEX).select('SOURCE_INDEX','COMPARISON_INDEX','PRODUCT_KEY').withColumnRenamed("PRODUCT_KEY", "COMPARISON_PRODUCT_KEY")

pairwise_product_df = source_df.join(comparison_df, (source_df.SOURCE_INDEX == comparison_df.SOURCE_INDEX) & (source_df.COMPARISON_INDEX == comparison_df.COMPARISON_INDEX)).select(source_df.SOURCE_INDEX, "SOURCE_PRODUCT_KEY", source_df.COMPARISON_INDEX, "COMPARISON_PRODUCT_KEY", "PRODUCT_SIMILARITY")

unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_PRODUCT_KEY != pairwise_product_df.COMPARISON_PRODUCT_KEY)

sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("PRODUCT_SIMILARITY"), desc("COMPARISON_PRODUCT_KEY")) #tiebreaker?

ranked_final_df = unranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking)) \
    .select('SOURCE_INDEX', 'SOURCE_PRODUCT_KEY', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK')

ranked_final_df = ranked_final_df.where(ranked_final_df.FINAL_SIMILARITY_RANK <= 50)

source_items_df = configs_df.alias("source_items")
comparison_items_df = configs_df.alias("comparison_items")

write_out_example_source = ranked_final_df.join(source_items_df, ranked_final_df.SOURCE_INDEX == source_items_df["INDEX"]) \
    .select('SOURCE_INDEX','SOURCE_PRODUCT_KEY','PRODUCT_VERSION','MARKET', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK',  'source_items.USER_SELECTIONS_CONFIG_ONLY') \
    .withColumnRenamed("USER_SELECTIONS_CONFIG_ONLY", "SOURCE_USER_SELECTIONS_CONFIG_ONLY") \
    .withColumnRenamed("PRODUCT_VERSION", "SOURCE_PRODUCT_VERSION") 

write_out_example_final = write_out_example_source.join(comparison_items_df, ranked_final_df.COMPARISON_INDEX == comparison_items_df["INDEX"]) \
    .select('SOURCE_USER_SELECTIONS_CONFIG_ONLY','SOURCE_PRODUCT_KEY','SOURCE_PRODUCT_VERSION','source_items.MARKET','SOURCE_INDEX','comparison_items.USER_SELECTIONS_CONFIG_ONLY', 'COMPARISON_PRODUCT_KEY','comparison_items.PRODUCT_VERSION','COMPARISON_INDEX', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK') \
    .withColumnRenamed("USER_SELECTIONS_CONFIG_ONLY", "COMPARISON_USER_SELECTIONS_CONFIG_ONLY") \
    .withColumnRenamed("PRODUCT_VERSION", "COMPARISON_PRODUCT_VERSION") 


write_out_example_final = write_out_example_final.withColumn("SOURCE_REFERENCE_KEY_NO_SIZE", struct(col("SOURCE_PRODUCT_KEY").alias("key"),col("SOURCE_PRODUCT_VERSION").alias("version"), col("SOURCE_USER_SELECTIONS_CONFIG_ONLY").alias("options")))

write_out_example_final = write_out_example_final.withColumn("COMPARISON_REFERENCE_KEY_NO_SIZE", struct(col("COMPARISON_PRODUCT_KEY").alias("key"),col("COMPARISON_PRODUCT_VERSION").alias("version"), col("COMPARISON_USER_SELECTIONS_CONFIG_ONLY").alias("options")))

write_out_example_final = write_out_example_final.select('MARKET','SOURCE_INDEX','SOURCE_PRODUCT_KEY','SOURCE_PRODUCT_VERSION','SOURCE_REFERENCE_KEY_NO_SIZE','COMPARISON_INDEX','COMPARISON_PRODUCT_KEY','COMPARISON_PRODUCT_VERSION','COMPARISON_REFERENCE_KEY_NO_SIZE','PRODUCT_SIMILARITY','FINAL_SIMILARITY_RANK').orderBy("SOURCE_PRODUCT_KEY","SOURCE_INDEX","FINAL_SIMILARITY_RANK","PRODUCT_SIMILARITY").cache()


write_out_example_final = write_out_example_final.withColumn(
    "SOURCE_REFERENCE_KEY_NO_SIZE", 
    to_json(col("SOURCE_REFERENCE_KEY_NO_SIZE"))
)

# COMMAND ----------

grouped_demo_example = write_out_example_final \
    .groupBy('SOURCE_REFERENCE_KEY_NO_SIZE') \
    .agg(collect_list('COMPARISON_REFERENCE_KEY_NO_SIZE').alias("PRODUCTS"))

grouped_demo_example = grouped_demo_example.withColumn("SOURCE_REFERENCE_KEY_NO_SIZE", from_json("SOURCE_REFERENCE_KEY_NO_SIZE", product_options_schema))


# COMMAND ----------

grouped_demo_example.display()

# COMMAND ----------

import boto3 
import pandas as pd

aws_role_session = "dna_ppp_temp_session_gravity"
aws_peresonalize_execution_role = 'arn:aws:iam::905666450942:role/service-role/AmazonPersonalize-ExecutionRole-1595956851346'
aws_personalize_role = dbutils.secrets.get(scope="personalizationProductRecommender", key="awsPersonalizeAccountRole")
aws_account_id = "905666450942"
client = boto3.client('sts')
StsResponse = client.assume_role(RoleArn=aws_personalize_role, RoleSessionName=aws_role_session)


# s3_client =boto3.client('s3',
#                      aws_access_key_id=StsResponse['Credentials']['AccessKeyId'],
#                         aws_secret_access_key=StsResponse['Credentials']['SecretAccessKey'],
#                         aws_session_token=StsResponse['Credentials']['SessionToken'])
# #from recommender.aws_service.s3_client import S3Client
# S3_BUCKET = "precs-product"

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', StsResponse['Credentials']['AccessKeyId'])
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', StsResponse['Credentials']['SecretAccessKey'])
sc._jsc.hadoopConfiguration().set('fs.s3a.session.token', StsResponse['Credentials']['SessionToken'])

# COMMAND ----------

print(StsResponse)

# COMMAND ----------

grouped_demo_example.coalesce(1).write.json("s3a://precs-product-recommendation-dev/hackaton/sample")

# COMMAND ----------

write_out_example_final.where(write_out_example_final.SOURCE_INDEX == '01d58ee0601ad531d35ad9fe0d10989f1f97767189fa7bc7b772a55ce2787be2').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write out example

# COMMAND ----------

configs_df = configs_df.repartition("INDEX")
pairwise_df = pairwise_df.repartition("SOURCE_INDEX", "COMPARISON_INDEX")

# COMMAND ----------

source_df = configs_df.join(pairwise_df, configs_df.INDEX == pairwise_df.SOURCE_INDEX).select('SOURCE_INDEX','PRODUCT_KEY', 'COMPARISON_INDEX', 'PRODUCT_SIMILARITY').withColumnRenamed("PRODUCT_KEY", "SOURCE_PRODUCT_KEY")

comparison_df = configs_df.join(pairwise_df, configs_df.INDEX == pairwise_df.COMPARISON_INDEX).select('SOURCE_INDEX','COMPARISON_INDEX','PRODUCT_KEY').withColumnRenamed("PRODUCT_KEY", "COMPARISON_PRODUCT_KEY")

pairwise_product_df = source_df.join(comparison_df, (source_df.SOURCE_INDEX == comparison_df.SOURCE_INDEX) & (source_df.COMPARISON_INDEX == comparison_df.COMPARISON_INDEX)).select(source_df.SOURCE_INDEX, "SOURCE_PRODUCT_KEY", source_df.COMPARISON_INDEX, "COMPARISON_PRODUCT_KEY", "PRODUCT_SIMILARITY")

unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_PRODUCT_KEY != pairwise_product_df.COMPARISON_PRODUCT_KEY)

sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("PRODUCT_SIMILARITY"), desc("COMPARISON_PRODUCT_KEY")) #tiebreaker?

ranked_final_df = unranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking)) \
    .select('SOURCE_INDEX', 'SOURCE_PRODUCT_KEY', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK')

source_items_df = items_df.alias("source_items")
comparison_items_df = items_df.alias("comparison_items")

write_out_example_source = ranked_final_df.join(source_items_df, ranked_final_df.SOURCE_INDEX == source_items_df["INDEX"]) \
    .select('SOURCE_INDEX','SOURCE_PRODUCT_KEY','PRODUCT_VERSION','MARKET', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK',  'source_items.USER_SELECTIONS',  'source_items.USER_SELECTIONS_CONFIG_ONLY', "SIZE") \
    .withColumnRenamed("USER_SELECTIONS", "SOURCE_USER_SELECTIONS") \
    .withColumnRenamed("USER_SELECTIONS_CONFIG_ONLY", "SOURCE_USER_SELECTIONS_CONFIG_ONLY") \
    .withColumnRenamed("PRODUCT_VERSION", "SOURCE_PRODUCT_VERSION") \
    .withColumnRenamed("SIZE", "SOURCE_SIZE")

write_out_example_final = write_out_example_source.join(comparison_items_df, ranked_final_df.COMPARISON_INDEX == comparison_items_df["INDEX"]) \
    .select('SOURCE_USER_SELECTIONS','SOURCE_USER_SELECTIONS_CONFIG_ONLY','SOURCE_PRODUCT_KEY','SOURCE_PRODUCT_VERSION','source_items.MARKET','SOURCE_INDEX', 'SOURCE_SIZE', 'comparison_items.USER_SELECTIONS','comparison_items.USER_SELECTIONS_CONFIG_ONLY', 'COMPARISON_PRODUCT_KEY','comparison_items.PRODUCT_VERSION','COMPARISON_INDEX', 'comparison_items.SIZE', 'PRODUCT_SIMILARITY', 'FINAL_SIMILARITY_RANK') \
    .withColumnRenamed("USER_SELECTIONS", "COMPARISON_USER_SELECTIONS") \
    .withColumnRenamed("USER_SELECTIONS_CONFIG_ONLY", "COMPARISON_USER_SELECTIONS_CONFIG_ONLY") \
    .withColumnRenamed("PRODUCT_VERSION", "COMPARISON_PRODUCT_VERSION") \
    .withColumnRenamed("SIZE", "COMPARISON_SIZE") 

write_out_example_final = write_out_example_final.withColumn("SOURCE_REFERENCE_KEY_FULL", struct(col("SOURCE_PRODUCT_KEY").alias("key"),col("SOURCE_PRODUCT_VERSION").alias("version"), col("SOURCE_USER_SELECTIONS").alias("options")))
write_out_example_final = write_out_example_final.withColumn("SOURCE_REFERENCE_KEY_NO_SIZE", struct(col("SOURCE_PRODUCT_KEY").alias("key"),col("SOURCE_PRODUCT_VERSION").alias("version"), col("SOURCE_USER_SELECTIONS_CONFIG_ONLY").alias("options")))

write_out_example_final = write_out_example_final.withColumn("COMPARISON_REFERENCE_KEY_FULL", struct(col("COMPARISON_PRODUCT_KEY").alias("key"),col("COMPARISON_PRODUCT_VERSION").alias("version"), col("COMPARISON_USER_SELECTIONS").alias("options")))
write_out_example_final = write_out_example_final.withColumn("COMPARISON_REFERENCE_KEY_NO_SIZE", struct(col("COMPARISON_PRODUCT_KEY").alias("key"),col("COMPARISON_PRODUCT_VERSION").alias("version"), col("COMPARISON_USER_SELECTIONS_CONFIG_ONLY").alias("options")))

write_out_example_final = write_out_example_final.select('MARKET','SOURCE_INDEX','SOURCE_PRODUCT_KEY','SOURCE_PRODUCT_VERSION','SOURCE_SIZE','SOURCE_REFERENCE_KEY_FULL','SOURCE_REFERENCE_KEY_NO_SIZE','COMPARISON_INDEX','COMPARISON_PRODUCT_KEY','COMPARISON_PRODUCT_VERSION','COMPARISON_SIZE','COMPARISON_REFERENCE_KEY_FULL','COMPARISON_REFERENCE_KEY_NO_SIZE','PRODUCT_SIMILARITY','FINAL_SIMILARITY_RANK').orderBy("SOURCE_PRODUCT_KEY","SOURCE_INDEX","FINAL_SIMILARITY_RANK","PRODUCT_SIMILARITY","SOURCE_SIZE","COMPARISON_SIZE").cache()

# COMMAND ----------

write_out_example_final.display()

# COMMAND ----------

33749 * 33749 = 1138995001 - 33749 = 1138961252
write_out_example_final.count()


# COMMAND ----------

write_out_example_final.where(write_out_example_final.SOURCE_PRODUCT_KEY == 'PRD-U8U4UG9AS').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top K

# COMMAND ----------

def topk_similar(similarity_df, feature_df, prd_index_list, k=5):

    source_df = feature_df.join(similarity_df, feature_df.INDEX == similarity_df.SOURCE_INDEX).select('SOURCE_INDEX','PRODUCT_KEY', 'COMPARISON_INDEX', 'PRODUCT_SIMILARITY','COLOUR_SIMILARITY').withColumnRenamed("PRODUCT_KEY", "SOURCE_PRODUCT_KEY")

    comparison_df = feature_df.join(similarity_df, feature_df.INDEX == similarity_df.COMPARISON_INDEX).select('SOURCE_INDEX','COMPARISON_INDEX','PRODUCT_KEY').withColumnRenamed("PRODUCT_KEY", "COMPARISON_PRODUCT_KEY")

    pairwise_product_df = source_df.join(comparison_df, (source_df.SOURCE_INDEX == comparison_df.SOURCE_INDEX) & (source_df.COMPARISON_INDEX == comparison_df.COMPARISON_INDEX)).select(source_df.SOURCE_INDEX, "SOURCE_PRODUCT_KEY", source_df.COMPARISON_INDEX, "COMPARISON_PRODUCT_KEY", "PRODUCT_SIMILARITY",'COLOUR_SIMILARITY')

    unranked_sim_df = pairwise_product_df.where(pairwise_product_df.SOURCE_PRODUCT_KEY != pairwise_product_df.COMPARISON_PRODUCT_KEY)

    comparison_prod_ranking = Window.partitionBy("SOURCE_INDEX","COMPARISON_PRODUCT_KEY").orderBy(asc("COLOUR_SIMILARITY"), "COMPARISON_INDEX")
    sim_ranking = Window.partitionBy("SOURCE_INDEX").orderBy(desc("PRODUCT_SIMILARITY"), "COMPARISON_INDEX")

    ranked_sim_df = unranked_sim_df.withColumn("COMPARISON_CONFIG_RANK", rank().over(comparison_prod_ranking))
    ranked_sim_df = ranked_sim_df.where(ranked_sim_df.COMPARISON_CONFIG_RANK == 1)
    ranked_final_df = ranked_sim_df.withColumn("FINAL_SIMILARITY_RANK", rank().over(sim_ranking)).select('SOURCE_INDEX','SOURCE_PRODUCT_KEY', 'COMPARISON_INDEX', 'COMPARISON_PRODUCT_KEY', 'PRODUCT_SIMILARITY','COLOUR_SIMILARITY', 'FINAL_SIMILARITY_RANK')

    output = ranked_final_df.where((ranked_final_df.SOURCE_INDEX.isin(prd_index_list)) & (ranked_final_df.FINAL_SIMILARITY_RANK <= k)).orderBy("SOURCE_INDEX","FINAL_SIMILARITY_RANK")
    return(output)

# COMMAND ----------

# lookup_items = ['f487351d0d76052ad8ac3b3aada3840b36188192f4ada01138ce56c7e6df50b4','92502f292542d0468a07d4259b25c4824bb67c0b1404b86a12790ecb8347faeb','dc88956a9019842b19bc10dcb1124a90344e3ea0ed947e6d62ae36dbff65566a','b5d9f799ecfd33e16fd16e2e80c645e43954ea54f3ea6a042a1559a13d23a1e4','220939d3c5340d911959475e6d1d5395050eb3e2064236b3a2051bb79f375664','c94c95a503e0fbc852ae5c788b554216e9abcfdbeb6b0544b8e73dd390faf8b3','b61234a1c142f111106087763216225089d35807b248350bef8c0c8c8b194028']
lookup_items = ['70960b2df95b46e159790821c284da96e4e06ebc65d36c055116756df82b6d25']

topk_example = topk_similar(pairwise_df, configs_df, lookup_items, 5)
topk_example.display()

# COMMAND ----------

def topk_manual_review(topk_df, feature_df):

    columns_to_compare = ['PRODUCT_KEY', 'CATEGORY', 'SUBCATEGORY', 'PRODUCT_GROUP', 'PRODUCT_NAME', 'GENDER', 'SLEEVE_STYLE', 'COLOUR', 'HUMAN_COLOR', 'BRAND_NAME', 'COLLAR_TYPE', 'DECORATION_LOCATION_1', 'DECORATION_TECHNOLOGY_1', 'BACKSIDE', 'MATERIAL','MOQ_UNIT_PRICE', 'MIN_QUANTITY']

    topk_source_attributes =  feature_df.join(topk_df, feature_df.INDEX == topk_df.SOURCE_INDEX)
    topk_comparison_attributes =  feature_df.join(topk_df, feature_df.INDEX == topk_df.COMPARISON_INDEX)

    num_cols = len(columns_to_compare)
    expr_str = "stack(" + str(num_cols) + ", " + ", ".join([f"'{col}', CAST({col} AS STRING)" for col in columns_to_compare]) + ") as (ATTRIBUTE, VALUE)"

    source_melted_df = topk_source_attributes.select("SOURCE_INDEX","SOURCE_PRODUCT_KEY",'COMPARISON_INDEX', "COMPARISON_PRODUCT_KEY", "PRODUCT_SIMILARITY", "COLOUR_SIMILARITY", "FINAL_SIMILARITY_RANK", expr(expr_str)).withColumnRenamed("VALUE", "SOURCE_ATTRIBUTE_VALUE")
    comparision_melted_df = topk_comparison_attributes.select("SOURCE_INDEX",'COMPARISON_INDEX', expr(expr_str)).withColumnRenamed("VALUE", "COMPARISON_ATTRIBUTE_VALUE")

    full_melted_df = source_melted_df.join(comparision_melted_df, (source_melted_df.SOURCE_INDEX == comparision_melted_df.SOURCE_INDEX) & \
                                            (source_melted_df.COMPARISON_INDEX == comparision_melted_df.COMPARISON_INDEX) & \
                                            (source_melted_df.ATTRIBUTE == comparision_melted_df.ATTRIBUTE)).select(source_melted_df.SOURCE_INDEX, source_melted_df.SOURCE_PRODUCT_KEY, source_melted_df.COMPARISON_INDEX, source_melted_df.COMPARISON_PRODUCT_KEY, source_melted_df.PRODUCT_SIMILARITY, source_melted_df.COLOUR_SIMILARITY, source_melted_df.FINAL_SIMILARITY_RANK, source_melted_df.ATTRIBUTE, source_melted_df.SOURCE_ATTRIBUTE_VALUE, comparision_melted_df.COMPARISON_ATTRIBUTE_VALUE) 
    
    return(full_melted_df)

# COMMAND ----------

topk_example_review = topk_manual_review(topk_example, configs_df)
topk_example_review.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Metric calculation**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Priority of features by Tiers:
# MAGIC
# MAGIC Tier 1:
# MAGIC SUBCATEGORY, PRODUCT_GROUP
# MAGIC
# MAGIC Tier 2:
# MAGIC GENDER, COLOUR, MIN_QUANTITY
# MAGIC
# MAGIC Tier 3:
# MAGIC SLEEVE_STYLE, BRAND_NAME, DECORATION_LOCATION_1, DECORATION_TECHNOLOGY_1
# MAGIC
# MAGIC Tier 4:
# MAGIC PRICE
# MAGIC
# MAGIC Tier 5:
# MAGIC BACK, COLLAR_TYPE, MATERIAL

# COMMAND ----------

# MAGIC %md
# MAGIC **Source products used for comparison**

# COMMAND ----------

#Product 1: PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#4e4e64' -> Woman's short sleeve T-shirt (grey/navy) 
#Product 2: PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#eff0f2' -> Youth T-shirt with 3/4 sleeve (light grey/white)
#Product 3: PRODUCT_KEY = 'PRD-XP37HM58I', COLOUR = '#F4633A' -> Man's sleeveless tank top with back (orange)
#Product 4: PRODUCT_KEY = 'PRD-N7VURKY0W', COLOUR = '#1b7ad8' -> Woman's Polo with right chest with None for sleeves (blue)
#Product 5: PRODUCT_KEY = 'PRD-XRTWOT9JG', COLOUR = '#b6263f' -> Man's T-shirt with left chest and long sleeves (red)

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually retrieved similar products to source Product 1 ordered in terms of similarity**

# COMMAND ----------

#Product 1: PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#4e4e64' -> Woman's short sleeve T-shirt (grey/navy)

#similar colour, same product
#PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#5a5254'
#PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#272323'
#PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#2c2b3c'
#PRODUCT_KEY = 'PRD-GURKDU3VZ', COLOUR = '#505fa0'

#back different, similar colour
#PRODUCT_KEY = 'PRD-0HBESMDP5', COLOUR = '#5a5254'
#PRODUCT_KEY = 'PRD-0HBESMDP5', COLOUR = '#272323'
#PRODUCT_KEY = 'PRD-0HBESMDP5', COLOUR = '#2c2b3c'
#PRODUCT_KEY = 'PRD-0HBESMDP5', COLOUR = '#505fa0'
#PRODUCT_KEY = 'PRD-0HBESMDP5', COLOUR = '#4e4e64'
 
#Decoration technology different, similar colour
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#4e4e64'
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#b7b3b0'
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#505fa0'
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#2c2b3c'
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#272323'
#PRODUCT_KEY = 'PRD-TTS0JM1VF', COLOUR = '#5a5254'

#Sleeve style different, similar colour
#PRODUCT_KEY = 'PRD-0JWZ1YHJJ', COLOUR = '#554c4f'
#PRODUCT_KEY = 'PRD-0JWZ1YHJJ', COLOUR = '#2b2727'
#PRODUCT_KEY = 'PRD-0JWZ1YHJJ', COLOUR = '#54546a'

#Collar type different, decoration location different
#PRODUCT_KEY = 'PRD-XMJYT4MI5', COLOUR = '#2c2b3d'
#PRODUCT_KEY = 'PRD-XMJYT4MI5', COLOUR = '#2b2726' 
#PRODUCT_KEY = 'PRD-XMJYT4MI5', COLOUR = '#46475d' 
#PRODUCT_KEY = 'PRD-XMJYT4MI5', COLOUR = '#564e51'
#PRODUCT_KEY = 'PRD-XMJYT4MI5', COLOUR = '#5565a4' 

#Collar type different, decoration technology different
#PRODUCT_KEY = 'PRD-ZG1WYLURC', COLOUR = '#554c4f'
#PRODUCT_KEY = 'PRD-ZG1WYLURC', COLOUR = '#2b2726' 
#PRODUCT_KEY = 'PRD-ZG1WYLURC', COLOUR = '#434459' 
#PRODUCT_KEY = 'PRD-ZG1WYLURC', COLOUR = '#333345'
#PRODUCT_KEY = 'PRD-ZG1WYLURC', COLOUR = '#4e5d9f'

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually retrieved similar products to source Product 2 ordered in terms of similarity**

# COMMAND ----------

#Product 2: PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#eff0f2' -> Youth T-shirt with 3/4 sleeve (light grey/white)

#similar colour, same product
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#e7e7e9'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#ebebed'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#e7e8ed'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#aeafb3'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#efefef'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#ededef'
#PRODUCT_KEY = 'PRD-VJEGZBENW', COLOUR = '#e7e8ec'

#similar colour, different sleeve length and partially similar material
#PRODUCT_KEY = 'PRD-TDJUKNKJT', COLOUR = '#c0bfc5'

#similar colour, different sleeve length, partially similar decoration location, partially similar material
#PRODUCT_KEY = 'PRD-EHUQQFXKW', COLOUR = '#EBEBEB'

#similar colour, different sleeve length, different brand, partially similar material
#PRODUCT_KEY = 'PRD-E9J8SM8FR', COLOUR = '#d8d8d8'
#PRODUCT_KEY = 'PRD-E9J8SM8FR', COLOUR = '#eaeaea'
#PRODUCT_KEY = 'PRD-E9J8SM8FR', COLOUR = '#e2e2e4'
#PRODUCT_KEY = 'PRD-POYEE2ODU', COLOUR = '#d4d1dc'
#PRODUCT_KEY = 'PRD-POYEE2ODU', COLOUR = '#e8e5ed'
#PRODUCT_KEY = 'PRD-POYEE2ODU', COLOUR = '#c1c1c1'
#PRODUCT_KEY = 'PRD-VBFQCXZJ6', COLOUR = '#ffffff'
#PRODUCT_KEY = 'PRD-VBFQCXZJ6', COLOUR = '#c6c6c8'
#PRODUCT_KEY = 'PRD-VCXDFQLAV', COLOUR = '#ebecee'
#PRODUCT_KEY = 'PRD-FC7ALFJY0', COLOUR = '#c4c5ca'
#PRODUCT_KEY = 'PRD-FC7ALFJY0', COLOUR = '#ededf0'
#PRODUCT_KEY = 'PRD-FC7ALFJY0', COLOUR = '#d2d1cd'
#PRODUCT_KEY = 'PRD-KDXIWGQ3G', COLOUR = '#f1f1f1'

#similar colour, different sleeve length, different brand, partially similar material, price 30% higher
#PRODUCT_KEY = 'PRD-DBRQCT4LI', COLOUR = '#f0f1f5'
#PRODUCT_KEY = 'PRD-DBRQCT4LI', COLOUR = '#cacaca'
#PRODUCT_KEY = 'PRD-DBRQCT4LI', COLOUR = '#ebecf0'

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually retrieved similar products to source Product 3 ordered in terms of similarity**

# COMMAND ----------

#Product 3: PRODUCT_KEY = 'PRD-XP37HM58I', COLOUR = '#F4633A' -> Man's sleeveless tank top with back (orange)

#similar colour, different sleeve length, partially similar collar type
#PRODUCT_KEY = 'PRD-IY7BBX8ZD', COLOUR = '#fd6e70'
#PRODUCT_KEY = 'PRD-IY7BBX8ZD', COLOUR = '#ff6d59'

#similar colour, different brand, different sleeve length, partially similar collar type 
#PRODUCT_KEY = 'PRD-RHQMTEUMM', COLOUR = '#F36C40'
#PRODUCT_KEY = 'PRD-RHQMTEUMM', COLOUR = '#FAA627'
#PRODUCT_KEY = 'PRD-RHQMTEUMM', COLOUR = '#B22038'

#similar colour, different back, different brand, very similar location, different decoration technology, price 25% higher
#PRODUCT_KEY = 'PRD-KAAXJGSE7', COLOUR = '#F4633A'
#PRODUCT_KEY = 'PRD-KAAXJGSE7', COLOUR = '#FF8A3D'

#similar colour, different back, different brand, very similar location, different decoration technology, different material, price 30% lower
#PRODUCT_KEY = 'PRD-ACDNKQC4M', COLOUR = '#fd8f6e'

#similar colour, different back, different brand, very similar location, different decoration technology, partially similar material, partially similar collar type
#PRODUCT_KEY = 'PRD-HI3JOJWWD', COLOUR = '#cf1e39'

#similar colour, different back, different brand, different location, different decoration technology, price 25% higher
#PRODUCT_KEY = 'PRD-MNAU8GBYS', COLOUR = '#fe6d58'
#PRODUCT_KEY = 'PRD-MNAU8GBYS', COLOUR = '#fd4974'

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually retrieved similar products to source Product 4 ordered in terms of similarity**

# COMMAND ----------

#Product 4: PRODUCT_KEY = 'PRD-N7VURKY0W', COLOUR = '#1b7ad8' -> Woman's Polo with right chest with None for sleeves (blue)
#CORE365 and CORE 365

#similar colour, different decoration location
#PRODUCT_KEY = 'PRD-2VB3IIX1J', COLOUR = '#1c73c2'
#PRODUCT_KEY = 'PRD-2VB3IIX1J', COLOUR = '#0ba4e7'
#PRODUCT_KEY = 'PRD-VXCB4LTTG', COLOUR = '#01529e'
#PRODUCT_KEY = 'PRD-HSSNR5RGJ', COLOUR = '#2fc1fc'
#PRODUCT_KEY = 'PRD-HSSNR5RGJ', COLOUR = '#0d5aa0'

#similar colour, different decoration location, different brand
#PRODUCT_KEY = 'PRD-BA7OJIFTS', COLOUR = '#008dd5'
#PRODUCT_KEY = 'PRD-0M8T70P7S', COLOUR = '#294084'
#PRODUCT_KEY = 'PRD-0M8T70P7S', COLOUR = '#749bc2'
#PRODUCT_KEY = 'PRD-UNJTHJVR1', COLOUR = '#005ab2'
#PRODUCT_KEY = 'PRD-BXD7XWNDP', COLOUR = '#2345b0'
#PRODUCT_KEY = 'PRD-DYYFTDL8H', COLOUR = '#0c55a4'
#PRODUCT_KEY = 'PRD-KVEIMGEYS', COLOUR = '#4a5eaf'
#PRODUCT_KEY = 'PRD-KVEIMGEYS', COLOUR = '#7b9ee0'

#similar colour, different decoration location, different brand, price >20% higher
#PRODUCT_KEY = 'PRD-XCWAEC6CX', COLOUR = '#7da0d6'
#PRODUCT_KEY = 'PRD-XCWAEC6CX', COLOUR = '#012b81'
#PRODUCT_KEY = 'PRD-HGNB5UULS', COLOUR = '#0c5a9c'

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually retrieved similar products to source Product 5 ordered in terms of similarity**

# COMMAND ----------

#Product 5: PRODUCT_KEY = 'PRD-XRTWOT9JG', COLOUR = '#b6263f' -> Man's T-shirt with left chest and long sleeves (red)

#similar colour, different sleeve length
#PRODUCT_KEY = 'PRD-ASID68BLC', COLOUR = '#f36b39'
#PRODUCT_KEY = 'PRD-ASID68BLC', COLOUR = '#a11e2b'

#similar colour, different brand
#PRODUCT_KEY = 'PRD-0NZLVK7YQ', COLOUR = '#d6312f'
#PRODUCT_KEY = 'PRD-0NZLVK7YQ', COLOUR = '#ff652d'

#similar colour, different brand, partially similar material
#PRODUCT_KEY = 'PRD-1SRJVPVAY', COLOUR = '#ff5c39'
#PRODUCT_KEY = 'PRD-2QGUEHDGI', COLOUR = '#dc3644'

#similar colour, partially similar collar type, sleeve style unknown
#PRODUCT_KEY = 'PRD-D3UCMCXRV', COLOUR = '#d7112a'
#PRODUCT_KEY = 'PRD-5UKHGJ8HS', COLOUR = '#ed1c24'
#PRODUCT_KEY = 'PRD-6COG2O1JY', COLOUR = '#ee6879'

#similar colour, partially similar collar type, partially similar material, sleeve style unknown
#PRODUCT_KEY = 'PRD-EJ81TQIC5', COLOUR = '#a1102f'
#PRODUCT_KEY = 'PRD-EJ81TQIC5', COLOUR = '#fe6d3f'

#similar colour, different collar type, sleeve style unknown
#PRODUCT_KEY = 'PRD-NLPVCI8GY', COLOUR = '#b72027'
#PRODUCT_KEY = 'PRD-ZWIQENOSD', COLOUR = '#b90934'

#similar colour, different collar type, sleeve style unknown, different material
#PRODUCT_KEY = 'PRD-PFCFOSBA7', COLOUR = '#dd0038'


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Function that retrieves similar items for a source product with cosine similarity using TOTAL_EMBEDDINGS**

# COMMAND ----------

# from sklearn.metrics.pairwise import cosine_similarity
# import numpy as np

# #full_df = items - here we need to either transform to pandas or adapt to pyspark
# items['similarity'] = None

# # Constructing embedding_index using (PRODUCT_KEY, COLOUR) as keys
# embedding_index = {}
# for index, row in items.iterrows():
#     key = (row['PRODUCT_KEY'], row['COLOUR'])
#     embedding_index[key] = row['TOTAL_EMBEDDINGS'] #full embedding

# # Function to find similar T-shirts based on (PRODUCT_KEY, COLOUR)
# def find_similar_tshirts(product_key, colour, embedding_index, top_n=200):
#     try:
#         item_embedding = embedding_index[(product_key, colour)]
#     except KeyError:
#         print(f"Embedding not found for PRODUCT_KEY={product_key}, COLOUR={colour}")
#         return []

#     similarities = {}

#     # Calculate cosine similarity with all other items
#     for (key, col), embedding in embedding_index.items():
#         similarity = cosine_similarity([item_embedding], [embedding])[0][0]
#         similarities[(key, col)] = similarity
    
#     # Sort the similarities dictionary by values in descending order
#     similar_tshirts = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    
#     # Exclude the input item itself from the list
#     similar_tshirts = [(item, similarity) for item, similarity in similar_tshirts if (item[0] != product_key or item[1] != colour)]
    
#     # Return the top N similar T-shirts
#     return similar_tshirts[:top_n]

# # Example: Find similar T-shirts for a given (PRODUCT_KEY, COLOUR)
# items['similarity'] = None
# product_key = 'PRD-GURKDU3VZ'
# colour = '#4e4e64'
# similar_tshirts = find_similar_tshirts(product_key, colour, embedding_index)

# # Update 'similarity' column with scores
# for (key, col), similarity in similar_tshirts:
#     items.loc[(items['PRODUCT_KEY'] == key) & (items['COLOUR'] == col), 'similarity'] = similarity

#     print(f"Similar T-shirts to PRODUCT_KEY={product_key}, COLOUR={colour}:")
# for (key, col), similarity in similar_tshirts:
#     print(f"PRODUCT_KEY={key}, COLOUR={col}, Similarity={similarity}")

# COMMAND ----------

# MAGIC %md
# MAGIC If you have a similarity score ranging from 0 to 1, you can omit this function above and use it as it

# COMMAND ----------

# MAGIC %md
# MAGIC **MANUAL SET OF SIMILAR ITEMS IN THE FORMAT OF LIST OF LISTS**

# COMMAND ----------

# #list of source products
# products_list = [
#     ['PRD-GURKDU3VZ', '#4e4e64'],
#     ['PRD-VJEGZBENW', '#eff0f2'],
#     ['PRD-XP37HM58I', '#F4633A'],
#     ['PRD-N7VURKY0W', '#1b7ad8'],
#     ['PRD-XRTWOT9JG', '#b6263f'] 
# ]

# # Manual set of relevant items
# manual_sets = [[
#     ('PRD-GURKDU3VZ', '#5a5254'), ('PRD-GURKDU3VZ', '#272323'), ('PRD-GURKDU3VZ', '#2c2b3c'), ('PRD-GURKDU3VZ', '#505fa0'),
#     ('PRD-0HBESMDP5', '#5a5254'), ('PRD-0HBESMDP5', '#272323'), ('PRD-0HBESMDP5', '#2c2b3c'), ('PRD-0HBESMDP5', '#505fa0'), ('PRD-0HBESMDP5', '#4e4e64'), ('PRD-TTS0JM1VF', '#4e4e64'), ('PRD-TTS0JM1VF', '#b7b3b0'), ('PRD-TTS0JM1VF', '#505fa0'), ('PRD-TTS0JM1VF', '#2c2b3c'), ('PRD-TTS0JM1VF', '#272323'), ('PRD-TTS0JM1VF', '#5a5254'), ('PRD-0JWZ1YHJJ', '#554c4f'), ('PRD-0JWZ1YHJJ', '#2b2727'), ('PRD-0JWZ1YHJJ', '#54546a'), ('PRD-XMJYT4MI5', '#2c2b3d'), ('PRD-XMJYT4MI5', '#2b2726'), ('PRD-XMJYT4MI5', '#46475d'), ('PRD-XMJYT4MI5', '#564e51'), ('PRD-XMJYT4MI5', '#5565a4'), ('PRD-ZG1WYLURC', '#554c4f'), ('PRD-ZG1WYLURC', '#2b2726'), ('PRD-ZG1WYLURC', '#434459'), ('PRD-ZG1WYLURC', '#333345'), ('PRD-ZG1WYLURC', '#4e5d9f')
# ],
#    [
#     ('PRD-VJEGZBENW', '#e7e7e9'),
#     ('PRD-VJEGZBENW', '#ebebed'),
#     ('PRD-VJEGZBENW', '#e7e8ed'),
#     ('PRD-VJEGZBENW', '#aeafb3'),
#     ('PRD-VJEGZBENW', '#efefef'),
#     ('PRD-VJEGZBENW', '#ededef'),
#     ('PRD-VJEGZBENW', '#e7e8ec'),
#     ('PRD-TDJUKNKJT', '#c0bfc5'),
#     ('PRD-EHUQQFXKW', '#EBEBEB'),
#     ('PRD-E9J8SM8FR', '#d8d8d8'),
#     ('PRD-E9J8SM8FR', '#eaeaea'),
#     ('PRD-E9J8SM8FR', '#e2e2e4'),
#     ('PRD-POYEE2ODU', '#d4d1dc'),
#     ('PRD-POYEE2ODU', '#e8e5ed'),
#     ('PRD-POYEE2ODU', '#c1c1c1'),
#     ('PRD-VBFQCXZJ6', '#ffffff'),
#     ('PRD-VBFQCXZJ6', '#c6c6c8'),
#     ('PRD-VCXDFQLAV', '#ebecee'),
#     ('PRD-FC7ALFJY0', '#c4c5ca'),
#     ('PRD-FC7ALFJY0', '#ededf0'),
#     ('PRD-FC7ALFJY0', '#d2d1cd'),
#     ('PRD-KDXIWGQ3G', '#f1f1f1'),
#     ('PRD-DBRQCT4LI', '#f0f1f5'),
#     ('PRD-DBRQCT4LI', '#cacaca'),
#     ('PRD-DBRQCT4LI', '#ebecf0')
# ],
#     [
#     ('PRD-IY7BBX8ZD', '#fd6e70'),
#     ('PRD-IY7BBX8ZD', '#ff6d59'),
#     ('PRD-RHQMTEUMM', '#F36C40'),
#     ('PRD-RHQMTEUMM', '#FAA627'),
#     ('PRD-RHQMTEUMM', '#B22038'),
#     ('PRD-KAAXJGSE7', '#F4633A'),
#     ('PRD-KAAXJGSE7', '#FF8A3D'),
#     ('PRD-ACDNKQC4M', '#fd8f6e'),
#     ('PRD-HI3JOJWWD', '#cf1e39'),
#     ('PRD-MNAU8GBYS', '#fe6d58'),
#     ('PRD-MNAU8GBYS', '#fd4974')
# ],
#     [
#     ('PRD-2VB3IIX1J', '#1c73c2'),
#     ('PRD-2VB3IIX1J', '#0ba4e7'),
#     ('PRD-VXCB4LTTG', '#01529e'),
#     ('PRD-HSSNR5RGJ', '#2fc1fc'),
#     ('PRD-HSSNR5RGJ', '#0d5aa0'),
#     ('PRD-BA7OJIFTS', '#008dd5'),
#     ('PRD-0M8T70P7S', '#294084'),
#     ('PRD-0M8T70P7S', '#749bc2'),
#     ('PRD-UNJTHJVR1', '#005ab2'),
#     ('PRD-BXD7XWNDP', '#2345b0'),
#     ('PRD-DYYFTDL8H', '#0c55a4'),
#     ('PRD-KVEIMGEYS', '#4a5eaf'),
#     ('PRD-KVEIMGEYS', '#7b9ee0'),
#     ('PRD-XCWAEC6CX', '#7da0d6'),
#     ('PRD-XCWAEC6CX', '#012b81'),
#     ('PRD-HGNB5UULS', '#0c5a9c')
# ],
#   [
#     ('PRD-ASID68BLC', '#f36b39'),
#     ('PRD-ASID68BLC', '#a11e2b'),
#     ('PRD-0NZLVK7YQ', '#d6312f'),
#     ('PRD-0NZLVK7YQ', '#ff652d'),
#     ('PRD-1SRJVPVAY', '#ff5c39'),
#     ('PRD-2QGUEHDGI', '#dc3644'),
#     ('PRD-D3UCMCXRV', '#d7112a'),
#     ('PRD-5UKHGJ8HS', '#ed1c24'),
#     ('PRD-6COG2O1JY', '#ee6879'),
#     ('PRD-EJ81TQIC5', '#a1102f'),
#     ('PRD-EJ81TQIC5', '#fe6d3f'),
#     ('PRD-NLPVCI8GY', '#b72027'),
#     ('PRD-ZWIQENOSD', '#b90934'),
#     ('PRD-PFCFOSBA7', '#dd0038')
# ]]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average Precision and Average NDCG

# COMMAND ----------

# def precision_at_k(df, k):
#     relevant_items = df.head(k)['RELEVANCE_PRECISION'].sum()
#     return relevant_items / k

# def dcg_at_k(r, k):
#     r = np.asfarray(r)[:k]
#     if r.size:
#         return np.sum(r / np.log2(np.arange(2, r.size + 2)))
#     return 0.0

# def ndcg_at_k(df, k):
#     # Sort by similarity score in descending order
#     df_sorted = df.sort_values(by='similarity', ascending=False).head(k)
    
#     # Relevance scores in sorted order
#     relevance_scores = df_sorted['RELEVANCE_NDCG']
    
#     # DCG for the top k items
#     dcg = dcg_at_k(relevance_scores, k)
    
#     # Ideal DCG (IDCG) for the top k items
#     ideal_relevance_scores = sorted(relevance_scores, reverse=True)
#     idcg = dcg_at_k(ideal_relevance_scores, k)
    
#     # NDCG is the ratio of DCG to IDCG
#     ndcg = dcg / idcg if idcg > 0 else 0.0
    
#     return ndcg


# COMMAND ----------

# precision_5 = []
# precision_10 = []
# precision_full = []

# ndcg_5 = []
# ndcg_10 = []
# ndcg_full = []

# count = 0
# for manual_set in manual_sets:

#     # Assign relevance scores
#     relevance_scores = list(range(len(manual_set), 0, -1))
#     manual_set_with_scores = dict(zip(manual_set, relevance_scores))

#     #function that retrieves similarity score, can input your own if have similarity already
#     #retrieve first n similar items (at_5, at_10)
#     items['similarity'] = None
#     similar_tshirts = find_similar_tshirts(products_list[count][0], products_list[count][1], embedding_index)

#     # Update 'similarity' column with scores
#     for (key, col), similarity in similar_tshirts:
#         items.loc[(items['PRODUCT_KEY'] == key) & (items['COLOUR'] == col), 'similarity'] = similarity

#     # Add relevance score to dataframe
#     items['RELEVANCE_NDCG'] = items.apply(lambda x: manual_set_with_scores.get((x['PRODUCT_KEY'], x['COLOUR']), 0), axis=1)
#     items['RELEVANCE_PRECISION'] = items.apply(lambda x: (x['PRODUCT_KEY'], x['COLOUR']) in manual_set, axis=1)
    
#     # Sort dataframe by similarity score in descending order
#     df_sorted = items.sort_values(by='similarity', ascending=False)

#     # Calculate precision at 5, 10, and full length of the manual set
#     precision_5.append(precision_at_k(df_sorted, 5))
#     precision_10.append(precision_at_k(df_sorted, 10))
#     precision_full.append(precision_at_k(df_sorted, len(manual_set)))
    
#     # Calculate NDCG at 5, 10, and full length of the manual set
#     ndcg_5.append(ndcg_at_k(items, 5))
#     ndcg_10.append(ndcg_at_k(items, 10))
#     ndcg_full.append(ndcg_at_k(items, len(manual_set)))

#     count+=1

# # Calculate average precision and NDCG
# avg_precision_5 = np.mean(precision_5)
# avg_precision_10 = np.mean(precision_10)
# #avg_precision_full = np.mean(precision_full)

# avg_ndcg_5 = np.mean(ndcg_5)
# avg_ndcg_10 = np.mean(ndcg_10)
# #avg_ndcg_full = np.mean(ndcg_full)

# print(f'Average Precision at 5: {avg_precision_5}')
# print(f'Average Precision at 10: {avg_precision_10}')
# #print(f'Average Precision at full length: {avg_precision_full}')

# print(f'Average NDCG at 5: {avg_ndcg_5}')
# print(f'Average NDCG at 10: {avg_ndcg_10}')
# #print(f'Average NDCG at full length: {avg_ndcg_full}')


# COMMAND ----------

# #DRAFT CODE TO CONSIDER COLOUR IN LAB FORMAT AS PART OF THE FINAL INDEX, TREAT AS NUMERICAL

# import tensorflow as tf
# from tensorflow.keras.models import Model
# from tensorflow.keras.layers import Input, Dense, Concatenate
# from sklearn.preprocessing import StandardScaler

# scaler_l = StandardScaler()
# scaler_a = StandardScaler()
# scaler_b = StandardScaler()

# scaler_moq = StandardScaler()

# # Assuming you have your data stored in a DataFrame called 'items'
# # Normalize numerical features
# items['L_COLOUR_SCALED'] = scaler_l.fit_transform(items['L_COLOR'].values.reshape(-1, 1))
# items['A_COLOUR_SCALED'] = scaler_a.fit_transform(items['A_COLOR'].values.reshape(-1, 1))
# items['B_COLOUR_SCALED'] = scaler_b.fit_transform(items['B_COLOR'].values.reshape(-1, 1))

# # Define input layers for numerical features
# input_l_colour = Input(shape=(1,), name='input_l')
# input_a_colour = Input(shape=(1,), name='input_a')
# input_b_colour = Input(shape=(1,), name='input_b')

# concatenated_inputs = Concatenate()([input_l_colour, input_a_colour, input_b_colour])

# # Define a series of dense layers to reduce dimensionality
# dense_layer = Dense(64, activation='relu')(concatenated_inputs)
# dense_layer = Dense(32, activation='relu')(dense_layer)

# # Define output layer
# output_layer = Dense(64, activation='linear')(dense_layer)

# # Create the model
# model = Model(inputs=[input_l_colour, input_a_colour, input_b_colour], outputs=output_layer)

# # Compile the model
# model.compile(optimizer='adam', loss='mse')

# # Extract embeddings for your data
# encoder_model = Model(inputs=model.input, outputs=model.layers[-2].output)

# # Get the embeddings
# embeddings = encoder_model.predict([items['L_COLOUR_SCALED'], items['A_COLOUR_SCALED'], items['B_COLOUR_SCALED']])

# # Add embeddings to the DataFrame
# items['NUMERICAL_EMBEDDINGS'] = list(embeddings)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
