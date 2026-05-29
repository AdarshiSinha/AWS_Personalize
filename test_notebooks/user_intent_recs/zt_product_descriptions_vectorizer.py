# Databricks notebook source
pip install -U sentence-transformers

# COMMAND ----------

artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

import io
import boto3 
import pandas as pd
from vp_dna import data_access_layer
from vista_dna_akeyless.akeyless_dna import AKeylessClient
import ast
import json
from sentence_transformers import SentenceTransformer
import numpy as np

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

def get_snowflake(environment):
    access_ids = {
        "dev": "p-60q2bjh345fy",
        "stg": "p-oium5k724hbj",
        "prd": "p-hbnpveay8mtx",
    }
    akeyless_client = AKeylessClient(access_id=access_ids[environment])
    sf_secret_path = f"/vistaprint/dna/ppp/team/precs/snowflake-product-recommendations-{environment}"
    akeyless_response = akeyless_client.get_secret_from_full_path(secret_path=sf_secret_path)
    snowflake_creds = ast.literal_eval(akeyless_response['snowflake'])
    client_id = snowflake_creds['username']
    client_secret = snowflake_creds['password']
    client_role = snowflake_creds['username'].split("_")[1]

    return data_access_layer.Snowflake(
        client_id=client_id,
        client_secret=client_secret,
        client_role=client_role,
        spark=spark,
        default_warehouse="RECOMMENDATIONS_DNA_WH",
        credentials_type='SNOWFLAKE',
        query_tag=json.dumps({
            "domain_name": "Marketing Optimization",
            "product_team_name": "CTAP",
            "data_product_name": "recommendations",
            "databricks_product_tag": "recommendations",
            "target_name": environment
        }))


# COMMAND ----------

snowflake = get_snowflake(environment)

# COMMAND ----------

query = '''SELECT DISTINCT PRODUCT_KEY, MPV_ID, PRODUCT_NAME, PRODUCT_GROUP, COUNTRY_CODE FROM VISTAPRINT.PRODUCT.PRODUCT_URL_MAPPING
WHERE COUNTRY_CODE = 'US'
AND PRODUCT_NAME IS NOT NULL
AND MPV_ID IS NOT NULL '''

df_spark = snowflake.execute_reader(query)

# COMMAND ----------

df_spark.count()

# COMMAND ----------

import pyspark.sql.functions as f
df_spark = df_spark.withColumn("INDEX", f.sha2(f.concat(*(f.col(c).cast("string") for c in ["PRODUCT_KEY"])), 256)).cache()
df_spark.display()

# COMMAND ----------

df_spark.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, ceil, percent_rank,from_json, explode,ntile, desc, asc, round, when, lit, pow, current_timestamp, log, monotonically_increasing_id, row_number, concat, array, struct, rank, dense_rank,  from_json, to_json, collect_list, transform,flatten, broadcast, regexp_replace, concat_ws

from pyspark.sql.types import *
from pyspark.sql.window import Window
import torch
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity

# COMMAND ----------

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
        #model = SentenceTransformer('mixedbread-ai/mxbai-embed-large-v1', truncate_dim=dimensions).cpu()
        model = SentenceTransformer("thenlper/gte-large").cpu()
        text_embedding = model.encode(text_array,batch_size=64)

        
        schema = StructType([
                    StructField("embeddings", ArrayType(FloatType()), True)])
        doc_vecs = text_embedding.tolist()
        b = spark.createDataFrame([(l,) for l in doc_vecs], schema)
        a = text_df.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
        b = b.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
        return a.join(b, a.row_idx == b.row_idx).\
             drop("row_idx")

# COMMAND ----------

# Add embeddings column to the DataFrame
spark_df_with_embeddings = prepare_embedding(df_spark, spark, col_to_embed='PRODUCT_NAME').cache()


# COMMAND ----------

spark_df_with_embeddings.display()

# COMMAND ----------

model = SentenceTransformer("thenlper/gte-large")

# COMMAND ----------

user_query = "I want a men's t-shirt"
user_query_embedding = np.array(model.encode([user_query])[0])

# COMMAND ----------

embeddings_array = np.array(spark_df_with_embeddings.select(col("embeddings")).rdd.flatMap(lambda x: x).collect())
index_array = np.array(spark_df_with_embeddings.select(col("INDEX")).rdd.flatMap(lambda x: x).collect()).reshape(-1)

# COMMAND ----------

emb_tensor = torch.tensor(embeddings_array, dtype=torch.float32)
if emb_tensor.ndimension() == 1:
    emb_tensor = emb_tensor.unsqueeze(0)


query_tensor = torch.tensor(user_query_embedding, dtype=torch.float32)
if query_tensor.ndimension() == 1:
    query_tensor = query_tensor.unsqueeze(0)

# COMMAND ----------

from scipy.sparse import coo_matrix

cosine_scores = util.cos_sim(query_tensor, emb_tensor)
scores_array_sparse = coo_matrix(cosine_scores.numpy().astype("float"))

# COMMAND ----------

scores_array_sparse.shape

# COMMAND ----------

sc = spark.sparkContext.getOrCreate()

comparison_schema = StructType([
    StructField("INDEX", StringType(), False),
    StructField("PRODUCT_SIMILARITY", FloatType(), False)
])

comparison_rdd = sc.parallelize(range(scores_array_sparse.nnz)).map(
    lambda idx: (
        str(index_array[scores_array_sparse.col[idx]]), 
        float(scores_array_sparse.data[idx])
    )
)


spark_df_with_similarity = spark.createDataFrame(comparison_rdd, comparison_schema)
spark_df_with_similarity = df_spark.join(spark_df_with_similarity, df_spark.INDEX == spark_df_with_similarity.INDEX).select('PRODUCT_KEY', 'MPV_ID', 'PRODUCT_NAME', 'PRODUCT_GROUP', 'COUNTRY_CODE', 'PRODUCT_SIMILARITY',df_spark.INDEX)


# COMMAND ----------

# Sort items by cosine similarity and get the top-k
k = 20
spark_df_top_k = spark_df_with_similarity.orderBy(col("PRODUCT_SIMILARITY").desc()).limit(k)
spark_df_top_k.display()

# COMMAND ----------

#Applying unique PRODUCT GROUP filter
# Sort the DataFrame by PRODUCT_GROUP and cosine_similarity in descending order
window_spec = Window.partitionBy("PRODUCT_GROUP").orderBy(col("PRODUCT_SIMILARITY").desc())

# Add a row number for each group
spark_df_with_similarity = spark_df_with_similarity.withColumn("PRODUCT_GROUP_RANK", row_number().over(window_spec))
# Filter out rows where row_num is greater than 1 (keeping only the first row for each PRODUCT_GROUP)
spark_df_filtered = spark_df_with_similarity.filter(col("PRODUCT_GROUP_RANK") == 1)#.drop("PRODUCT_GROUP_RANK")

spark_df_filtered = spark_df_filtered.orderBy(col("PRODUCT_SIMILARITY").desc())

spark_df_filtered.display()
