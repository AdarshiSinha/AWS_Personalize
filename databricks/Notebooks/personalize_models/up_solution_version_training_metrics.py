# Databricks notebook source
# DBTITLE 1,6  v
artifactory_user = dbutils.secrets.get(scope="dna_public", key="artifactory_user")
artifactory_password = dbutils.secrets.get(scope="dna_public", key="artifactory_password")
%pip install --index-url "https://{artifactory_user}:{artifactory_password}@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple" vp-dna==2.1.0 vista_dna_akeyless==1.0.8
%pip install boto3 --upgrade

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

if environment == "dev":
    db = "sandbox"
    schema = "dna_personalization_dev"
elif environment == "stg":
    db = "sandbox"
    schema = "dna_personalization_stg"
elif environment == "prd":
    db = "dna"
    schema = "personalization"
else:
    raise Exception("Environment should be dev, stg or prd")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import expr, split, col, row_number, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

utils = Utils(spark, dbutils)

snowflake = utils.get_snowflake()
temp_creds = utils.get_temp_aws_credentials()
aws_session = utils.get_temp_session(temp_creds)
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

dataset_groups_response = personalize.list_dataset_groups()

dataset_group_arns = []

# filter test dataset groups
for dg_arn in dataset_groups_response['datasetGroups']:
    try:
        dg_tag = personalize.list_tags_for_resource(resourceArn=dg_arn['datasetGroupArn'])["tags"][0]["tagValue"]
        if 'test' not in dg_tag.lower():
            dataset_group_arns.append(dg_arn['datasetGroupArn'])

    except:
        pass

# COMMAND ----------

def get_up_solution_versions(dataset_group_arn_list):
    solution_version_arns = []
    for dg_arn in dataset_group_arn_list:
        solutions_response = personalize.list_solutions(datasetGroupArn=dg_arn)
        up_solutions = [sol for sol in solutions_response['solutions'] if 'up_sol' in sol['solutionArn'].lower()]
        if not up_solutions:
            continue
        solution_arn = up_solutions[0]['solutionArn']
        solutions_version_response = personalize.list_solution_versions(solutionArn=solution_arn)
        try:
            next_token = solutions_version_response.get('nextToken')
            while next_token:
                additional_solution_version_data = personalize.list_solution_versions(solutionArn=solution_arn, nextToken=next_token)
                solutions_version_response['solutionVersions'] += additional_solution_version_data['solutionVersions']
                next_token = additional_solution_version_data.get('nextToken')
        except:
            pass
        for sv in solutions_version_response['solutionVersions']:
            sv['datasetGroupArn'] = dg_arn
            solution_version_arns.append(sv)
    return solution_version_arns

# COMMAND ----------

solutions_version_arns = get_up_solution_versions(dataset_group_arns)

# COMMAND ----------

solution_version_arn_cols = ['solutionVersionArn','creationDateTime','status']

# COMMAND ----------

rows = []
for i in solutions_version_arns:
    row_dict = {col: i.get(col, None) for col in solution_version_arn_cols}
    rows.append(Row(**row_dict))

solutions_verion_arns_df = spark.createDataFrame(rows)

# COMMAND ----------

solutions_verion_arns_df = solutions_verion_arns_df.withColumn('solutionVersion', expr(f"substring(solutionVersionArn, 1, length(solutionVersionArn) - 9)"))
solutions_verion_arns_df = solutions_verion_arns_df.where(solutions_verion_arns_df.status == 'ACTIVE')

# COMMAND ----------

window_spec = Window.partitionBy("solutionVersion").orderBy(col("creationDateTime").desc())

current_solution_version_arn_df = solutions_verion_arns_df.withColumn("solution_recency_rank", row_number().over(window_spec)) \
                                         .filter(col("solution_recency_rank") == 1)
current_solution_version_arn_list = current_solution_version_arn_df.select('solutionVersionArn').rdd.flatMap(lambda x: x).collect()

current_solution_version_arn_df.display()

# COMMAND ----------

metrics_schema = StructType([
    StructField("solutionVersionArn", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_score", DoubleType(), True)
])

metrics_df = spark.createDataFrame([], metrics_schema)

for arn in current_solution_version_arn_list:
    solution_metrics = personalize.get_solution_metrics(solutionVersionArn=arn)
    solutionVersionArn = solution_metrics['solutionVersionArn']
    metrics = solution_metrics['metrics']
    
    metrics_list = [(solutionVersionArn, k, v) for k, v in metrics.items()]
    
    temp_df = spark.createDataFrame(metrics_list, metrics_schema)
    
    metrics_df = metrics_df.union(temp_df)

metrics_df = metrics_df.withColumn('solutionVersion', expr(f"substring(solutionVersionArn, 1, length(solutionVersionArn) - 9)"))
split_cols = split(metrics_df['solutionVersionArn'], '/')
metrics_df = metrics_df.withColumn('solution', split_cols.getItem(1)) \
                       .withColumn('solution_version', split_cols.getItem(2)).select('solutionVersionArn','solution','solution_version','metric_name','metric_score')


# COMMAND ----------

metrics_df = metrics_df.join(current_solution_version_arn_df, on='solutionVersionArn').select('solution','solution_version','metric_name','metric_score','creationDateTime')
metrics_df = metrics_df.withColumn('created_at', to_date(col('creationDateTime'))).drop('creationDateTime')
metrics_df.display()

# COMMAND ----------

snowflake.execute_nonquery(f"""
CREATE TABLE IF NOT EXISTS {db}.{schema}.aws_up_metrics (
    SOLUTION                                VARCHAR(16777216),
    SOLUTION_VERSION                        VARCHAR(16777216),
    METRIC_NAME                             VARCHAR(16777216),
    METRIC_SCORE                            NUMBER(10,5),
    CREATED_AT                              DATE
    )
CLUSTER BY (SOLUTION)
""")


# COMMAND ----------

snowflake.table_from_df(
    df=metrics_df,
    database=db,
    schema=schema,
    table="aws_up_metrics_temp",
    mode="overwrite")

# COMMAND ----------

snowflake.execute_nonquery(f"""
MERGE INTO {db}.{schema}.aws_up_metrics as target
USING {db}.{schema}.aws_up_metrics_temp as source
ON source.SOLUTION = target.SOLUTION
AND source.METRIC_NAME = target.METRIC_NAME
AND source.CREATED_AT = target.CREATED_AT
WHEN MATCHED THEN UPDATE SET
    target.SOLUTION_VERSION = source.SOLUTION_VERSION,
    target.METRIC_SCORE = source.METRIC_SCORE
WHEN NOT MATCHED THEN 
    INSERT (SOLUTION, SOLUTION_VERSION, METRIC_NAME, METRIC_SCORE, CREATED_AT)
    VALUES (source.SOLUTION, source.SOLUTION_VERSION, source.METRIC_NAME, source.METRIC_SCORE, source.CREATED_AT)
""")
