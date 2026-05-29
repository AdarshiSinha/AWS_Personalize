# Databricks notebook source
artifact_user = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryUser")
artifact_password = dbutils.secrets.get(scope="personalizationProductRecommender", key="artifactoryPwd")

artifactory_url_virtual = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple"
artifactory_url_local = f"https://{artifact_user}:{artifact_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-local/simple"

%pip install --index-url {artifactory_url_virtual} vp-dna==2.1.0 vista_dna_akeyless==1.0.8

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

import numpy as np
import io
import pandas as pd
import os
import sys
import boto3 
from vp_dna import data_access_layer
from vista_dna_akeyless.akeyless_dna import AKeylessClient
import json
import pyspark.sql.functions as f
from datetime import timedelta, datetime, timezone
from pyspark.sql.window import Window
from pyspark.sql.functions import sequence, explode, col, row_number, month, year, from_unixtime, date_sub, current_date, date_trunc
import ast
from typing import List, Tuple, Optional

# COMMAND ----------

# MAGIC %run ../../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
session = utils.get_temp_session(temp_creds)
personalize_runtime_client = utils.get_personalize_runtime_client(temp_creds)
personalize = utils.get_personalize_client(temp_creds)
s3_client = utils.get_s3_client(temp_creds)

# COMMAND ----------

class S3DatasetFetcher:
    """
    Flexible fetcher for legacy (v3, v4) and current pipeline data from S3.
    Supports date ranges that may span across multiple pipeline versions.
    """

    def __init__(self, s3_client, spark, bucket: str, dataset_groups: dict):
        self.s3_client = s3_client
        self.spark = spark
        self.bucket = bucket
        self.dataset_groups = dataset_groups

        # Version cut-off dates
        self.V3_CUTOFF = datetime(2024, 4, 9, tzinfo=timezone.utc)
        self.V4_CUTOFF = datetime(2025, 5, 29, tzinfo=timezone.utc)

    def fetch(
        self,
        regions: List[str],
        datasets: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[Optional["DataFrame"], Optional["DataFrame"]]:

        items_pd, users_pd = [], []

        for region in regions:
            # Iterate across ALL prefixes that might intersect the requested date range
            possible_prefixes = self._get_possible_prefixes(region, start_date, end_date)

            for prefix in possible_prefixes:
                print(f"[INFO] Processing region '{region}' prefix: {prefix}")
                for obj in self._list_s3_objects(prefix):
                    if start_date <= obj["LastModified"] <= end_date:
                        # Choose processing method per object
                        if "v3" in prefix or "v4" in prefix:
                            self._process_legacy(obj, region, datasets, items_pd, users_pd)
                        else:
                            self._process_new_pipeline(obj, region, datasets, items_pd, users_pd)

        items_df = self.spark.createDataFrame(pd.concat(items_pd, ignore_index=True)) if items_pd else None
        users_df = self.spark.createDataFrame(pd.concat(users_pd, ignore_index=True)) if users_pd else None
        return items_df, users_df

    # -------------
    # Prefix logic
    # -------------
    def _get_possible_prefixes(
        self,
        region: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[str]:
        """
        Returns all prefixes that could be relevant for the given date range
        across v3, v4, and new pipeline.
        """
        prefixes = []

        # If range overlaps v3
        if start_date <= self.V3_CUTOFF and end_date >= datetime.min.replace(tzinfo=timezone.utc):
            prefixes.append(f"dna-ppp-product-recommender-data-product/pipeline/{region}/v3/features/")

        # If range overlaps v4
        if end_date >= self.V3_CUTOFF and start_date <= self.V4_CUTOFF:
            prefixes.append(f"dna-ppp-product-recommender-data-product/pipeline/{region}/v4/features/")

        # If range includes new pipeline era
        if end_date > self.V4_CUTOFF:
            prefixes.append(f"dna-ppp-product-recommender-data-product/new_pipeline/{self.dataset_groups[region]}/")

        return prefixes

    # -------------
    # S3 interaction
    # -------------
    def _list_s3_objects(self, prefix: str) -> List[dict]:
        """List S3 objects for the given prefix (no pagination for simplicity)."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return response.get("Contents", [])

    def _read_csv_from_s3(self, key: str) -> pd.DataFrame:
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))

    # -------------
    # Processing logic
    # -------------
    def _process_legacy(self, obj: dict, region: str, datasets: List[str], items_pd: list, users_pd: list):
        """Process v3/v4 style pipeline outputs."""
        key = obj["Key"]
        if "items" in datasets and key.endswith("item_level_features.csv"):
            df = self._read_csv_from_s3(key)
            self._add_metadata(df, region, obj["LastModified"])
            items_pd.append(df)

        if "users" in datasets and key.endswith("shopper_features.csv"):
            df = self._read_csv_from_s3(key)
            self._add_metadata(df, region, obj["LastModified"])
            users_pd.append(df)

    def _process_new_pipeline(self, obj: dict, region: str, datasets: List[str], items_pd: list, users_pd: list):
        """Process new pipeline outputs (explicit csv files)."""
        key = obj["Key"]
        if "items" in datasets and key.endswith("items.csv"):
            df = self._read_csv_from_s3(key)
            self._add_metadata(df, region, obj["LastModified"])
            items_pd.append(df)

        if "users" in datasets and key.endswith("users.csv"):
            df = self._read_csv_from_s3(key)
            self._add_metadata(df, region, obj["LastModified"])
            users_pd.append(df)

    def _add_metadata(self, df: pd.DataFrame, region: str, last_modified):
        df["REGION"] = region
        df["DATE"] = last_modified

# COMMAND ----------

version_to_include = ['v5']

# COMMAND ----------

dataset_groups_response = personalize.list_dataset_groups()
dataset_groups = {}

for dg in dataset_groups_response['datasetGroups']:
    try:
        arn = dg['datasetGroupArn']
        name = dg['name']
        tag = personalize.list_tags_for_resource(resourceArn=arn)["tags"][0]["tagValue"]
        if 'test' not in tag.lower():
            region, model_version, version = name.split("_")
            if version in version_to_include:
                dataset_groups[region] = name
    except:
        pass

dataset_groups

# COMMAND ----------

# MAGIC %md
# MAGIC #### Items & User

# COMMAND ----------

region_list = ['us','ca']
#region_list = list(dataset_groups.keys()) 
region_list

# COMMAND ----------

fetcher = S3DatasetFetcher(s3_client, spark, "precs-product-recommendation", dataset_groups)

# COMMAND ----------

items_df_apr, users_df_apr = fetcher.fetch(
    regions= region_list,
    datasets= ["items"],
    start_date= datetime(2025, 4, 13, tzinfo=timezone.utc),
    end_date= datetime(2025, 4, 20, 23, 59, 59, tzinfo=timezone.utc)
)

# COMMAND ----------

items_df_apr.groupby(['DATE','REGION']).count().display()

# COMMAND ----------

products_of_interest = ['deluxeCottonTote','classicCottonToteBagSmall', 'premiumZipToteBags']

# COMMAND ----------

pop_totes_apr_df = items_df_apr.where(items_df_apr['ITEM_ID'].isin(products_of_interest)) \
                        .select("REGION",
                                "PRODUCT_GROUP",
                                "DATE",
                                "ITEM_ID",
                                "IS_ACCESSORY", 
                                "IS_LOGOMAKER_ENABLED",
                                'DESIGN_COMPLEXITY_PROXY_CATEGORY',
                                'BULK_PROXY_CATEGORY',
                                'RATING_PROXY_CATEGORY',
                                'CALC_PRODUCT_RATING',
                                'HIGH_VALUE_PROXY_CATEGORY',
                                'DEFAULT_MOQ',
                                'MIN_UNIT_PRICE').orderBy("REGION", "ITEM_ID", "DATE")
pop_totes_apr_df.display()

# COMMAND ----------

items_df_jul, users_df_jul = fetcher.fetch(
    regions= region_list,
    datasets= ["items"],
    start_date= datetime(2025, 7, 6, tzinfo=timezone.utc),
    end_date= datetime(2025, 7, 20, 23, 59, 59, tzinfo=timezone.utc)
)

# COMMAND ----------

pop_totes_jul_df = items_df_jul.where(items_df_jul['ITEM_ID'].isin(products_of_interest)) \
                        .select("REGION",
                                "PRODUCT_GROUP",
                                "DATE",
                                "ITEM_ID",
                                "IS_ACCESSORY", 
                                "IS_LOGOMAKER_ENABLED",
                                'DESIGN_COMPLEXITY_PROXY_CATEGORY',
                                'BULK_PROXY_CATEGORY',
                                'RATING_PROXY_CATEGORY',
                                'CALC_PRODUCT_RATING',
                                'HIGH_VALUE_PROXY_CATEGORY',
                                'DEFAULT_MOQ',
                                'MIN_UNIT_PRICE').orderBy("REGION", "ITEM_ID", "DATE")
pop_totes_jul_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactions

# COMMAND ----------

def load_interaction_parts(spark, s3_first_part_path: str):
    """
    Given an S3 path to a single part file, loads all CSV parts
    from the same folder, appends them as pandas DataFrames, and returns a single Spark DataFrame.
    """

    # Remove 's3://' and split bucket from key
    s3_path = s3_first_part_path.replace("s3://", "")
    bucket, *key_parts = s3_path.split("/")
    key = "/".join(key_parts)

    # Get the folder prefix (everything up to the last '/')
    folder_prefix = os.path.dirname(key) + "/"

    # List all CSV files in the folder
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)
    all_csv_paths = [
        obj['Key']
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    if not all_csv_paths:
        raise ValueError("No CSV files found in the specified folder.")

    lista_interactions = []
    for csv_key in all_csv_paths:
        obj = s3_client.get_object(Bucket=bucket, Key=csv_key)
        df_part = pd.read_csv(io.BytesIO(obj['Body'].read()))
        lista_interactions.append(df_part)
    interactions_raw = pd.concat(lista_interactions, ignore_index=True)

    df = spark.createDataFrame(interactions_raw)
    return df

# COMMAND ----------

incremental_part_uri = "s3://precs-product-recommendation/dna-ppp-product-recommender-data-product/v5_exports/AMZN_Personalize/AMZN_Personalize/arn:aws:personalize:eu-west-1:905666450942:dataset-group-uk_20250218164316_v5/arn:aws:personalize:eu-west-1:905666450942:dataset-uk_20250218164316_v5-INTERACTIONS/export_job_uk/2025-07-03/part-00000-d8abdfff-7891-4c13-a1d8-5f19e80ba198-c000.csv"

interactions_incremental = load_interaction_parts(spark, incremental_part_uri).withColumn(
                                "date", from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd")
                                ).withColumn("real_timestamp",  from_unixtime(col("timestamp") / 1000).cast("timestamp")).cache()


# bulk_part_uri = "s3://precs-product-recommendation/dna-ppp-product-recommender-data-product/v5_exports/AMZN_Personalize/AMZN_Personalize/arn:aws:personalize:eu-west-1:905666450942:dataset-group-uk_20250218164316_v5/arn:aws:personalize:eu-west-1:905666450942:dataset-uk_20250218164316_v5-INTERACTIONS/export_job_uk/2025-07-03/part-00000-d8abdfff-7891-4c13-a1d8-5f19e80ba198-c000.csv"

# interactions_bulk = load_interaction_parts(spark, bulk_part_uri).withColumn(
#                                 "date", from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd")
#                                 ).withColumn("real_timestamp",  from_unixtime(col("timestamp") / 1000).cast("timestamp")).cache()
# interactions_raw = interactions_incremental.union(interactions_bulk).cache()

# COMMAND ----------

interactions_incremental.display()

# COMMAND ----------

distinct_event_types = interactions_incremental.select("event_type").distinct()
display(distinct_event_types)

# COMMAND ----------

recent_interactions = interactions_incremental.filter(
    (year(col("real_timestamp")) == 2025) & 
    (month(col("real_timestamp")).isin(3, 4, 5))
)
grouped_data = recent_interactions.groupBy("event_type", "locale", "item_id").count()



window_spec = Window.partitionBy("event_type", "locale").orderBy(col("count").desc())
ranked_data = grouped_data.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 20)

# COMMAND ----------

ranked_data.display()

# COMMAND ----------

print(date_sub(current_date(), 12))

# COMMAND ----------

vrecent_interactions = interactions_incremental.filter(col("real_timestamp") >= date_sub(current_date(), 100))
vgrouped_data = vrecent_interactions.groupBy("event_type", "locale", "item_id").count()

vwindow_spec = Window.partitionBy("event_type", "locale").orderBy(col("count").desc())
vranked_data = vgrouped_data.withColumn("rank", row_number().over(vwindow_spec)).filter(col("rank") <= 20)

# COMMAND ----------

vranked_data.display()

# COMMAND ----------

products_of_interest

# COMMAND ----------

filtered_data = vrecent_interactions.where(
    (vrecent_interactions.locale == 'GB') &
    (vrecent_interactions.item_id.isin(products_of_interest)) 
)

grouped_by_hour = filtered_data.groupBy(vrecent_interactions.item_id, date_trunc('hour', 'real_timestamp').alias('hour')).count()
grouped_by_hour.display()
