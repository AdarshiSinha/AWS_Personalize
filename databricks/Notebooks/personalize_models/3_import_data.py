# Databricks notebook source
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

from datetime import datetime, timezone
from io import StringIO
from time import sleep

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# ## The following lines can be uncommented for local testing or development.
# ## They set default values for Databricks widgets to simulate job parameters.

# dbutils.widgets.text("environment", "dev")
# dbutils.widgets.text("dataset_type", "items")
# dbutils.widgets.text("dataset_group_name", "anzs_20250212172516_v5")
# dbutils.widgets.text("limit_data", "False")

# COMMAND ----------

# This notebook will be executed during model creation and during daily items/users dataset update.
# So dataset_arn and dataset_group_name can be passed as an input from previous tasks (in case of model creation)
# and as job param (in case of items/users dataset update).

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("dataset_type", "")
dataset_type = dbutils.widgets.get("dataset_type")
if dataset_type not in ("items", "users", "interactions"):
    raise Exception("Invalid dataset type")

dbutils.widgets.text("dataset_group_name", "")
dataset_group_name = dbutils.widgets.get("dataset_group_name")
if not dataset_group_name:
    dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

dbutils.widgets.text("region", dataset_group_name.split("_")[0])
region = dbutils.widgets.get("region")
if not region:
    raise ValueError("Region not defined")

dbutils.widgets.text("version", dataset_group_name.split("_")[-1])
version = dbutils.widgets.get("version")
if not version:
    raise ValueError("Version not defined")

dbutils.widgets.text("limit_data", "False")
limit_data = dbutils.widgets.get("limit_data")
if limit_data == "True":
    limit = 'TOP 2000'
else:
    limit = ''

#default to midnight
dbutils.widgets.text("cutoff_time_string", "")
cutoff_time_string = dbutils.widgets.get("cutoff_time_string")
cutoff_time_string = datetime.now(timezone.utc).strftime("%Y%m%d") + "000000" if cutoff_time_string=='' else cutoff_time_string

print(f"""
environment: {environment}
dataset_group_name: {dataset_group_name}
region: {region}
version: {version}
dataset_type: {dataset_type}
limit_data: {limit_data}
cutoff_time_string: {cutoff_time_string}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
aws_session = utils.get_temp_session(temp_creds)
s3_resource = aws_session.resource("s3")

current_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
dataset_s3_prefix = f"dna-ppp-product-recommender-data-product/new_pipeline/{dataset_group_name}/{dataset_type}"
file_s3_prefix = f"{dataset_s3_prefix}/{current_ts}"
dataset_arn = f"arn:aws:personalize:eu-west-1:905666450942:dataset/{dataset_group_name}/{dataset_type.upper()}"
s3_bucket = utils.s3_bucket
sf_db = utils.sf_db
sf_schema = utils.sf_schema

#This is to enable the databricks utils functions.  
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", temp_creds["aws_access_key_id"])
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", temp_creds["aws_secret_access_key"])
sc._jsc.hadoopConfiguration().set("fs.s3a.session.token", temp_creds["aws_session_token"])

print(f"""
dataset_arn: {dataset_arn}
dataset_s3_prefix: {dataset_s3_prefix}
file_s3_prefix: {file_s3_prefix}
s3_bucket: {s3_bucket}
db: {sf_db}
schema: {sf_schema}
""")

# COMMAND ----------

def get_file_name_list(dataset_type, s3_dataset_group_path, top_k=1, cutoff_time_string=None):
    """
    Return up to `top_k` file paths for a given dataset type ('items', 'users', 'interactions'),
    sorted reverse-chronologically by the file's modificationTime.

    Assumes files are stored in S3 under:
      {s3_dataset_group_path}/{dataset_type}/{YYYYMMDDHHMMSS}/{dataset_type}.csv

    Args:
        dataset_type: One of {'items','users','interactions'}.
        s3_dataset_group_path: S3 path to the dataset group root (without trailing slash or dataset type).
        top_k: Max number of file paths to return.
        cutoff_time_string: If provided, only include files with date < cutoff_time_string.

    Returns:
        A list of up to `top_k` file paths (strings), newest first.
    """
    valid_types = {"items", "users", "interactions"}
    if dataset_type not in valid_types:
        raise ValueError(f"Invalid dataset_type: {dataset_type}. Must be one of {valid_types}.")
    
    s3_dir = f"{s3_dataset_group_path}/{dataset_type}"

    return_list = []
    try:
        entrie_list = dbutils.fs.ls(s3_dir)
    except:
        return return_list
    for e in entrie_list:
        path = f"{e[0]}{dataset_type}.csv"
        created_time = Utils.unix_time_to_str((dbutils.fs.ls(f"{e[0]}{dataset_type}.csv")[0].modificationTime))
        return_list.append((path, created_time))
    if cutoff_time_string:
        return_list = [e for e in return_list if e[1] < cutoff_time_string]

    return_list.sort(key=lambda x: x[1], reverse=True)
    return_list=[e for e in return_list[:top_k]]

    return return_list

# COMMAND ----------

def check_counts(df):
    bucket = s3_resource.Bucket(utils.s3_bucket)
    previous_files = list(bucket.objects.filter(Prefix=dataset_s3_prefix))
    if previous_files:
        previous_file = previous_files[-1]
        print(f"Previous file location: {utils.s3_bucket}/{previous_file.key}")
        previous_file_body = previous_file.get()['Body'].read().decode('utf-8')
        prev_dataset_count = len(previous_file_body.splitlines()) - 1
        current_dataset_count = df.count()
        print(f"Previous dataset count: {prev_dataset_count}, current dataset count: {current_dataset_count}")
        if abs(current_dataset_count - prev_dataset_count) / prev_dataset_count > 0.05:
            raise ValueError("The current dataset count differs from the previous dataset count by more than 5%")

def check_nulls(df, col):
    nulls = df.filter(df[col].isNull())
    has_nulls = nulls.count() > 0
    if has_nulls:
        nulls.display()
        raise ValueError(f"Column '{col}' has nulls")

def check_null_column(df, col):
    all_nulls = df.filter(df[col].isNotNull()).count() == 0
    if all_nulls:
        raise ValueError(f"Column '{col}' contains only nulls")

def check_duplicates(df, col):
    duplicates = df.groupBy(col).count().filter(f"count > 1").cache()
    has_duplicates = duplicates.count() > 0
    if has_duplicates:
        duplicates.display()
        raise ValueError(f"Column '{col}' has duplicates")

def check_values(df, col, values):
    print(f"Column '{col}' values and counts")
    df.groupBy(col).count().display()
    has_invalid_values = df.where(f"{col} not in {tuple(values)}").count() > 0
    if has_invalid_values:
        raise ValueError(f"Column '{col}' has invalid values, expected values: {values}")
    
def check_exact_values(df, col, values):
    print(f"Column '{col}' values and counts")
    df.groupBy(col).count().display()
    distinct_values = df.select(col).distinct()
    distinct_values_set = set([r[col] for r in distinct_values.collect()])
    expected_values_set = set(values)
    if distinct_values_set != expected_values_set:
        distinct_values.display()
        raise ValueError(f"Column '{col}' should contain only these values: {values}")

def negative_values_check(df, col):
    negative_values = df.filter(df[col] < 0)
    has_negative_values = negative_values.count() > 0
    if has_negative_values:
        negative_values.display()
        raise ValueError(f"Column '{col}' has negative values")

def check_missing_features(df, dataset_type, region, file_time, file_source):

    valid_types = {"items", "users", "interactions"}
    if dataset_type not in valid_types:
        raise ValueError(f"Invalid dataset_type: {dataset_type}. Must be one of {valid_types}.")

    if 'region' not in df.columns:
        df = df.withColumn('region', f.lit(region))

    if dataset_type == 'items':
        item_key_list = ['region','category','subcategory','product_group','item_id']
        item_feature_list = [
            "is_logomaker_enabled",
            "design_complexity_proxy_category",
            "bulk_proxy_category",
            "rating_proxy_category",
            "calc_product_rating",
            "high_value_proxy_category",
            "default_moq",
            "min_unit_price"]

        key_list = item_key_list
        feature_list = item_feature_list
    
    elif dataset_type == 'users':
        raise Exception("Not implemented")

    elif dataset_type == 'interactions':
        raise Exception("Not implemented")

    df = (utils.lowercase_all(df)
          .select(
              f.lit(file_time).alias('file_date'),
              *key_list,
              *[
                f.when(
                    f.col(col_name).isNull() | (f.length(f.col(col_name)) < 1), 1
                ).otherwise(0).alias(f"{col_name}_missing")
                for col_name in feature_list
              ],
            f.lit(file_source).alias('file_source')
          )
         )
    return df

def detect_newly_missing_item_features(df_history, 
                                       df_latest, 
                                       missing_suffix='_missing'):
    
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    item_key_list = ['region','category','subcategory','product_group','item_id']
    item_feature_list = [
                "is_logomaker_enabled",
                "design_complexity_proxy_category",
                "bulk_proxy_category",
                "rating_proxy_category",
                "calc_product_rating",
                "high_value_proxy_category",
                "default_moq",
                "min_unit_price"]
    
    schema = StructType([
        StructField('region', StringType(), False),
        StructField('category', StringType(), True),
        StructField('subcategory', StringType(), True),
        StructField('product_group', StringType(), True),
        StructField('item_id', StringType(), True),
        StructField('feature_name', StringType(), False),
        StructField('is_missing', IntegerType(), False)
    ])
    df_item_missing_report = spark.createDataFrame([], schema=schema)
    missing_feature_list = [f"{e}_missing" for e in item_feature_list]

    for e in missing_feature_list:
        feature_name = e.replace(f'{missing_suffix}', '')
        df_old = df_history.where(f.col(e)==0).select(*item_key_list, e)
        df_new = df_latest.where(f.col(e)==1).select(*item_key_list, e)
        df_tmp = (df_old.select(item_key_list).join(df_new, on=item_key_list, how='inner')
                .withColumn('feature_name', f.lit(feature_name))
                .withColumnRenamed(e, f"is_missing")
                .select(df_item_missing_report.columns))
        
        print(f"processing feature {e}...had before but not now::", df_tmp.count())    
        df_item_missing_report = df_item_missing_report.union(df_tmp)

    df_item_missing_report = (df_item_missing_report
                            .withColumn('created_at', f.current_timestamp())                          
                            )
    return df_item_missing_report

def qa_users(dataset):
    qa_users_dataset = spark.createDataFrame(dataset).selectExpr(
        'USER_ID',
        'LIFECYCLE_SEGMENT',
        'TOTAL_ORDER_COUNT',
        'MASTER_SEGMENT',
        'CURRENT_OPT_STATUS',
        'int(BOOKING_BUDGET_CATEGORY) as BOOKING_BUDGET_CATEGORY',
        'int(IS_DIGITAL_PURCHASER) as IS_DIGITAL_PURCHASER',
        'INDUSTRY'
    ).cache()
    print("Users dataset sample")
    qa_users_dataset.display()
    check_counts(qa_users_dataset)
    check_nulls(qa_users_dataset, 'USER_ID')
    check_nulls(qa_users_dataset, 'LIFECYCLE_SEGMENT')
    check_nulls(qa_users_dataset, 'TOTAL_ORDER_COUNT')
    check_duplicates(qa_users_dataset, 'USER_ID')
    check_values(qa_users_dataset, 'MASTER_SEGMENT', ['Business', 'Consumer', 'Hybrid', 'Unknown'])
    check_exact_values(qa_users_dataset, 'BOOKING_BUDGET_CATEGORY', [1, 2, 3, 4, 5, 6, 7])
    check_exact_values(qa_users_dataset, 'IS_DIGITAL_PURCHASER', [0, 1])
    check_values(qa_users_dataset, 'CURRENT_OPT_STATUS', ['Unknown', 'opted in', 'opted out', 'opted down'])
    check_values(qa_users_dataset, 'INDUSTRY', [
        'Entertainment and Recreation',
        'Household Services',
        'Finance and Insurance',
        'Beauty and Spa',
        'Other',
        'Automotive and Transportation',
        'Manufacturing and Distribution',
        'Travel and Accommodations',
        'Health and Fitness',
        'Arts, Crafts, and Design',
        'Food and Beverage',
        'Technology',
        'Retail',
        'Professional Services',
        'Education Services',
        'Religious and Spiritual',
        'Non-Profits, Charity, and Politics',
        'Construction and Real Estate',
        'Agriculture and Farming',
        'Animal and Pet Care',
        'Unknown'
    ])
    qa_users_dataset.unpersist()

def qa_items(dataset):
    qa_items_dataset = spark.createDataFrame(dataset).selectExpr(
        'ITEM_ID',
        'SITE_CATEGORY',
        'int(IS_ACCESSORY) as IS_ACCESSORY',
        'int(IS_LOGOMAKER_ENABLED) as IS_LOGOMAKER_ENABLED',
        'DESIGN_COMPLEXITY_PROXY_CATEGORY',
        'HIGH_VALUE_PROXY_CATEGORY',
        'RATING_PROXY_CATEGORY',
        'BULK_PROXY_CATEGORY',
        'MIN_UNIT_PRICE'
    ).cache()
    print("Items dataset sample")
    qa_items_dataset.display()
    check_counts(qa_items_dataset)
    check_duplicates(qa_items_dataset, 'ITEM_ID')
    check_nulls(qa_items_dataset, 'ITEM_ID')
    check_nulls(qa_items_dataset, 'IS_ACCESSORY')
    check_nulls(qa_items_dataset, 'IS_LOGOMAKER_ENABLED')
    negative_values_check(qa_items_dataset, 'MIN_UNIT_PRICE')
    check_exact_values(qa_items_dataset, 'IS_ACCESSORY', [0, 1])
    check_exact_values(qa_items_dataset, 'IS_LOGOMAKER_ENABLED', [0, 1])
    check_values(qa_items_dataset, 'DESIGN_COMPLEXITY_PROXY_CATEGORY', ['1_high', '2_medium', '3_low', '4_least','5_unknown'])
    check_values(qa_items_dataset, 'BULK_PROXY_CATEGORY', ['1_high', '2_medium', '3_low', '4_least','5_unknown'])
    check_values(qa_items_dataset, 'RATING_PROXY_CATEGORY', ['1_high', '2_medium', '3_low', '4_least','5_unknown'])
    check_values(qa_items_dataset, 'HIGH_VALUE_PROXY_CATEGORY', ['1_high', '2_medium', '3_low', '4_least','5_unknown'])
    for col in qa_items_dataset.columns:
        check_null_column(qa_items_dataset, col)
    qa_items_dataset.unpersist()

def qa_items_missing_features(df_dataset,
                             query, 
                             dataset_group_name=dataset_group_name, 
                             cutoff_time_string=cutoff_time_string,
                             s3_bucket=s3_bucket, 
                             region=region):
    """
    1. Get the most recent historical items file from S3 before the cutoff time
    2. If no history file is found, exit early
    3. Load the historical items file as a Spark DataFrame
    4. Generate missing feature indicators for the historical items data
    5. Generate missing feature indicators for the latest items data
    6. Detect items that now have newly missing features compared to history
    7. Display the report of newly missing item features    
    """
    
    s3_dataset_group_path = f"s3://{s3_bucket}/dna-ppp-product-recommender-data-product/new_pipeline/{dataset_group_name}/"
    history_file_list = get_file_name_list(dataset_type='items',
                                            s3_dataset_group_path=s3_dataset_group_path,
                                            top_k=1,
                                            cutoff_time_string=cutoff_time_string)

    if len(history_file_list) == 0:
        return print("No history file found...")

    history_file_path = history_file_list[0][0]
    history_file_time = history_file_list[0][1]
    df_history = spark.read.csv(history_file_path, header=True, inferSchema=True)

    df_item_history_missing = check_missing_features(df=df_history, 
                                                dataset_type='items',
                                                region=region, 
                                                file_time=history_file_time,
                                                file_source=history_file_path)

    df_item_latest_missing = check_missing_features(df=df_dataset, 
                                                dataset_type='items',
                                                region=region, 
                                                file_time=cutoff_time_string,
                                                file_source=query)

    df_item_missing_report = detect_newly_missing_item_features(df_history=df_item_history_missing, 
                                                                df_latest=df_item_latest_missing)
    return df_item_missing_report.display()

def qa_interactions(dataset):
    print("Interactions dataset sample")
    dataset.display()
    check_nulls(dataset, 'ITEM_ID')
    check_nulls(dataset, 'USER_ID')
    check_nulls(dataset, 'TIMESTAMP')
    check_nulls(dataset, 'EVENT_TYPE')
    check_values(dataset, 'PAGE_SECTION', [
        "Product Page",
        "Studio",
        "Gallery",
        "Configure - Recommendation",
        "qr-code-wrap-up",
        "Cart",
        "Home Page",
        "unknown",
        "Search Results Page",
        "Internal Error",
        "My Account",
        "Confirmation",
        "Design Services"
    ])
    check_exact_values(dataset, 'EVENT_TYPE', definitions[version][dataset_type]["event_types"])

def save_pandas_dataset(pd_dataset, file_path, s3_resource):
    csv_buffer = StringIO()
    pd_dataset.to_csv(csv_buffer, index=False)
    print(f"Saving dataset to {utils.s3_bucket}/{file_path}")
    s3_resource.Object(utils.s3_bucket, file_path).put(Body=csv_buffer.getvalue())

# COMMAND ----------

# MAGIC %run ./load_versions

# COMMAND ----------

# Interactions is the largest dataset, and it is returned as a Spark dataframe.
# Historically we save Pandas dataframes as CSV files, so we need to convert Spark df to Pandas df.
# For that we split Spark df in 10 parts, convert each part to Pandas df and save it to S3.
# Items and users are small datasets, so they are returned as Pandas df and saved to S3 without splitting.

locales = utils.region_locale[region]
locales_tuple = tuple(locales) if len(locales) > 1 else f"('{locales[0]}')"
query_template = definitions[version][dataset_type]["query"]

if dataset_type == "interactions":
    query = query_template.format(locales=locales_tuple, limit_filter=limit)
    print(query)
    dataset = snowflake.execute_reader(query).cache()
    qa_interactions(dataset)
    interactions_dataset_parts = dataset.randomSplit([0.1]*10)
    for part_index, part in enumerate(interactions_dataset_parts):
        interactions_dataset_part_pd = part.toPandas()
        file_path = f"{file_s3_prefix}/{dataset_type}_{part_index}.csv"
        save_pandas_dataset(interactions_dataset_part_pd, file_path, s3_resource)
    dataset.unpersist()

if dataset_type == "items":
    query = query_template.format(region=region, sf_db=sf_db, sf_schema=sf_schema)
    print(query)
    df_dataset = snowflake.execute_reader(query)
    dataset = df_dataset.toPandas()
    qa_items(dataset)
    qa_items_missing_features(df_dataset=df_dataset, query=query)
    save_pandas_dataset(dataset, f"{file_s3_prefix}/{dataset_type}.csv", s3_resource)

if dataset_type == "users":
    query = query_template.format(locales=locales_tuple)
    print(query)
    df_dataset = snowflake.execute_reader(query)
    dataset = df_dataset.toPandas()
    qa_users(dataset)
    save_pandas_dataset(dataset, f"{file_s3_prefix}/{dataset_type}.csv", s3_resource)

# COMMAND ----------

if environment in ("stg", "prd"):

    temp_creds = utils.get_temp_aws_credentials()
    personalize = utils.get_personalize_client(temp_creds)
    full_dataset_path = f"s3://{utils.s3_bucket}/{file_s3_prefix}"
    import_name = f"{dataset_group_name}_{dataset_type}_import_{current_ts}"
    aws_personalize_role = dbutils.secrets.get(
        scope="personalizationProductRecommender",
        key="awsPersonalizeAccountRole")

    print(f"Data will be imported from {full_dataset_path}")

    # AWS Personalize has a limit of 5 dataset import jobs running at the same time,
    # so we need to wait if the limit is reached.
    import_job_arn = None
    for attempt in range(1, 11):
        try:
            import_job_arn = personalize.create_dataset_import_job(
                jobName=import_name,
                datasetArn=dataset_arn,
                dataSource={"dataLocation": full_dataset_path},
                roleArn=aws_personalize_role
            )["datasetImportJobArn"]
            break
        except personalize.exceptions.LimitExceededException:
            print(f"Attempt {attempt} failed due to import limit, waiting for 5 minutes")
            sleep(300)

    status = None
    while status != "ACTIVE":
        status = personalize.describe_dataset_import_job(
            datasetImportJobArn=import_job_arn
        )["datasetImportJob"]["status"]
        if status == "CREATE FAILED":
            raise Exception(f"Error while creating {import_job_arn}")
        sleep(30)
