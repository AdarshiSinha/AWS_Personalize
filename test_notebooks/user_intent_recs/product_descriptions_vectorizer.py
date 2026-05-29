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
df_descriptions = df_spark.toPandas()

# COMMAND ----------

df_descriptions.head()

# COMMAND ----------

print(len(df_descriptions))

# COMMAND ----------

model = SentenceTransformer("thenlper/gte-large")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer

# Create a Pandas UDF for batch encoding
@pandas_udf(ArrayType(FloatType()))
def encode_names_udf(names: pd.Series) -> pd.Series:
    # Batch encode names
    embeddings = model.encode(names.tolist())
    # Convert embeddings to numpy arrays (or pandas Series)
    return pd.Series([embedding.tolist() for embedding in embeddings])


spark_df = df_spark.repartition(200)

# Add embeddings column to the DataFrame
spark_df_with_embeddings = spark_df.withColumn(
    "embeddings", encode_names_udf(col("PRODUCT_NAME"))
)


# COMMAND ----------

spark_df_with_embeddings.where(spark_df_with_embeddings.PRODUCT_KEY == 'PRD-BNCH3NL62').display()

# COMMAND ----------

#df_descriptions = spark_df_with_embeddings.toPandas()

# COMMAND ----------

from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import ArrayType, FloatType
import numpy as np
from sentence_transformers import SentenceTransformer
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

# UDF to encode user query
@udf(ArrayType(FloatType()))
def encode_user_query_udf(query: str):
    return model.encode([query])[0].tolist()

# UDF to calculate cosine similarity (uses dot product and norm)
@udf(FloatType())
def cosine_similarity_udf(vec1, vec2):
    dot_product = float(np.dot(vec1, vec2))
    norm1 = float(np.linalg.norm(vec1))
    norm2 = float(np.linalg.norm(vec2))
    return dot_product / (norm1 * norm2)

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Broadcast the user query embedding to all worker nodes for faster processing
user_query = "I want to open a bakery"
user_query_embedding = model.encode([user_query])[0].tolist()

# Broadcast the user query embedding
user_query_broadcast = spark.sparkContext.broadcast(user_query_embedding)

# COMMAND ----------

# UDF to calculate cosine similarity using PySpark's Vectors
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import numpy as np

spark_df_with_embeddings = spark_df_with_embeddings.repartition(400)

@udf(FloatType())
def cosine_similarity_udf(vec1):
    query_vec = np.array(user_query_broadcast.value)
    dot_product = float(np.dot(vec1, query_vec))
    norm1 = float(np.linalg.norm(vec1))
    norm2 = float(np.linalg.norm(query_vec))
    return dot_product / (norm1 * norm2)

# Apply cosine similarity
spark_df_with_similarity = spark_df_with_embeddings.withColumn(
    "cosine_similarity", cosine_similarity_udf(col("embeddings"))
)

# Sort items by cosine similarity and get the top-k
k = 20
spark_df_top_k = spark_df_with_similarity.orderBy(col("cosine_similarity").desc()).limit(k)

# COMMAND ----------

#only descriptions
spark_df_top_k.display()

# COMMAND ----------

#everything
spark_df_top_k.display()

# COMMAND ----------

#Only product names
spark_df_top_k.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

#Applying unique PRODUCT GROUP filter
# Sort the DataFrame by PRODUCT_GROUP and cosine_similarity in descending order
window_spec = Window.partitionBy("PRODUCT_GROUP").orderBy(col("cosine_similarity").desc())

# Add a row number for each group
spark_df_with_similarity = spark_df_with_similarity.withColumn("row_num", row_number().over(window_spec))

# Filter out rows where row_num is greater than 1 (keeping only the first row for each PRODUCT_GROUP)
spark_df_filtered = spark_df_with_similarity.filter(col("row_num") == 1).drop("row_num")

spark_df_filtered = spark_df_filtered.orderBy(col("cosine_similarity").desc())

spark_df_filtered.display()
