# Databricks notebook source
import boto3
import ast
import json
import logging
import pyspark.sql.functions as f
import inspect

logging.getLogger("py4j").setLevel(logging.ERROR)

class Utils:

    def __init__(self, spark, dbutils):
        self.dbutils = dbutils
        self.spark = spark

        self.environment = dbutils.widgets.get("environment")
        if self.environment == "dev":
            self.akeyless_access_id = "p-60q2bjh345fy"
            self.s3_bucket = "precs-product-recommendation-dev"
            self.product = "dna_e2_dev_productrecommendations_a0ce4e86_2360_460e_8100_81a39afe3cf0"
            self.version_campaign_filter_map_table = "version_campaign_filter_map_dev"
            self.locale_version_tracker_map_table = "locale_version_tracker_map_dev"
            self.sf_db = "sandbox"
            self.sf_schema = "dna_personalization_dev"
        elif self.environment == "stg":
            self.akeyless_access_id = "p-oium5k724hbj"
            self.s3_bucket = "precs-product-recommendation-stg"
            self.product = "vista_stg_dna_ppp_product_recomme_e481bcfd_9a77_4e0c_90bb_56f5d3644c20"
            self.version_campaign_filter_map_table = "version_campaign_filter_map_dev"
            self.locale_version_tracker_map_table = "locale_version_tracker_map_dev"
            self.sf_db = "sandbox"
            self.sf_schema = "dna_personalization_stg"
        elif self.environment == "prd":
            self.akeyless_access_id = "p-hbnpveay8mtx"
            self.s3_bucket = "precs-product-recommendation"
            self.product = "vista_prd_dna_ppp_product_recomme_a6f65d5f_cc45_4930_95d5_7b4533446ae7"
            self.version_campaign_filter_map_table = "version_campaign_filter_map_prd"
            self.locale_version_tracker_map_table = "locale_version_tracker_map_prd"
            self.sf_db = "dna"
            self.sf_schema = "personalization"
        else:
            raise Exception("Environment should be dev, stg or prd")

        self.region_locale = {
            "frcen": ["FR", "BE", "NL"],
            "anzs": ["AU", "NZ", "SG"],
            "dach": ["DE", "AT", "CH"],
            "eu": ["ES", "FI", "SE", "IT", "DK", "PT", "NO"],
            "uk": ["GB", "IE"],
            "us": ["US"],
            "ca": ["CA"],
            "in": ["IN"],
        }

        self.region_code = {
            "GB": "en-gb",
            "IE": "en-ie",
            "ES": "es-es",
            "IT": "it-it",
            "PT": "pt-pt",
            "DK": "da-dk",
            "SE": "sv-se",
            "NO": "nb-no",
            "FI": "fi-fi",
            "CA": "en-ca",
            "US": "en-us",
            "AU": "en-au",
            "NZ": "en-nz",
            "SG": "en-sg",
            "FR": "fr-fr",
            "NL": "nl-nl",
            "BE": "nl-be",
            "DE": "de-de",
            "AT": "de-at",
            "CH": "de-ch",
            "IN": "en-in",
        }
 
    def get_auth0_token(self):
        from vista_dna_akeyless.akeyless_dna import AKeylessClient

        akeyless_client = AKeylessClient(access_id=self.akeyless_access_id)
        auth0_secret_path = f"/vistaprint/dna/ppp/team/precs/precs-airflow-deployments"
        akeyless_response = akeyless_client.get_secret_from_full_path(secret_path=auth0_secret_path)
        auth0_token = akeyless_response.get('access_token')

        return auth0_token
    
    def get_snowflake(self):

        from vp_dna import data_access_layer
        from vista_dna_akeyless.akeyless_dna import AKeylessClient

        akeyless_client = AKeylessClient(access_id=self.akeyless_access_id)
        sf_secret_path = f"/vistaprint/dna/ppp/team/precs/snowflake-product-recommendations-{self.environment}"
        akeyless_response = akeyless_client.get_secret_from_full_path(secret_path=sf_secret_path)
        snowflake_creds = ast.literal_eval(akeyless_response["snowflake"])
        client_id = snowflake_creds["username"]
        client_secret = snowflake_creds["password"]
        client_role = snowflake_creds["username"].split("@")[0]

        return data_access_layer.Snowflake(
            client_id=client_id,
            client_secret=client_secret,
            client_role=client_role,
            spark=self.spark,
            default_warehouse="RECOMMENDATIONS_DNA_WH",
            credentials_type="SNOWFLAKE",
            query_tag=json.dumps({
                "domain_name": "Personalization",
                "product_team_name": "PRECS",
                "data_product_name": "recommendations",
                "databricks_product_tag": "recommendations",
                "target_name": self.environment
            }))

    def get_temp_aws_credentials(self, duration=3600):
        """
        Credentials will be valid for 4 hours, if you want to change it:
        1. Change it in https://us-east-1.console.aws.amazon.com/iam/home?region=eu-west-1#/roles/details/precs-aws-personalize-role
           Look for "Maximum session duration"
        2. Then change it assume_role method below (DurationSeconds param)
        """
        aws_personalize_role = self.dbutils.secrets.get(
            scope="personalizationProductRecommender",
            key="awsPersonalizeAccountRole")
        sts_client = boto3.client("sts")
        sts_response = sts_client.assume_role(
            RoleArn=aws_personalize_role,
            RoleSessionName="recs",
            DurationSeconds=duration)
        return {
            "aws_access_key_id": sts_response["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": sts_response["Credentials"]["SecretAccessKey"],
            "aws_session_token": sts_response["Credentials"]["SessionToken"],
        }

    def get_personalize_client(self, temp_creds):
        return boto3.client("personalize", "eu-west-1", **temp_creds)

    def get_personalize_runtime_client(self, temp_creds):
        return boto3.client("personalize-runtime", "eu-west-1", **temp_creds)

    def get_s3_client(self, temp_creds):
        # this to enable in spark reading from recommendations s3 buckets
        sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", temp_creds["aws_access_key_id"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", temp_creds["aws_secret_access_key"])
        sc._jsc.hadoopConfiguration().set("fs.s3a.session.token", temp_creds["aws_session_token"])
        return boto3.client("s3", **temp_creds)

    def get_temp_session(self, temp_creds):
        return boto3.Session(**temp_creds)

    @staticmethod
    def qa_uniqueness(df_str, key_col_list):
        """
        Checks for uniqueness of the specified key columns in the given Spark DataFrame.
        Raises a ValueError if duplicate keys are found.
        
        Args:
            df_str (str): The variable name of the DataFrame to check, as a string.
            key_col_list (list): List of column names to check for uniqueness.
        """
        callers_local_vars = inspect.currentframe().f_back.f_locals.items()
        df = None
        for var_name, var_val in callers_local_vars:
            if var_name == df_str:
                df = var_val
                break

        row_count = df.count()
        unique_count = df.select(key_col_list).distinct().count()
        print(f"[QA] {df_str}: {unique_count} uniques records on the combinations {key_col_list}")
        if row_count != unique_count:
            raise ValueError("duplicate key(s) found!")

    @staticmethod
    def lowercase_all(df):
        """
        Returns a new Spark DataFrame with all column names converted to lowercase.

        Args:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with lowercase column names.
        """
        return df.select([f.col(x).alias(x.lower()) for x in df.columns])
    
    @staticmethod
    def unix_time_to_str(unix_time):
        from datetime import datetime    
        if len(str(unix_time)) == 13:
            return datetime.utcfromtimestamp(unix_time / 1000).strftime("%Y%m%d%H%M%S")
        elif len(str(unix_time)) == 10:
            return datetime.utcfromtimestamp(unix_time).strftime("%Y%m%d%H%M%S")
        else:
            raise ValueError("unix_time must be either 10 or 13 digits long")


    @staticmethod
    def assert_no_duplicates(df, key_col_list):
        """
        Raises a ValueError if duplicate rows are found for the specified columns in the given Spark DataFrame.

        Args:
            df (DataFrame): Input Spark DataFrame.
            key_col_list (list): List of column names to check for duplicates.
        """
        dup_count = df.groupBy(key_col_list).count().filter(f.col("count") > 1).cache()
        if dup_count.count() > 0:
            dup_count.display()
            raise ValueError(f"Duplicates found for columns: {key_col_list}")
        dup_count.unpersist()

# COMMAND ----------


