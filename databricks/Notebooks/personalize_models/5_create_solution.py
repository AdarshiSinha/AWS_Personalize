# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from time import sleep

dbutils.widgets.text("solution", "")
solution = dbutils.widgets.get("solution")

if solution not in ("sims", "popular", "up"):
    raise Exception(f"Invalid solution {solution}")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

dataset_group_arn = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_arn")
dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")
create_solution_args = None

# COMMAND ----------

if solution == "popular":
    create_solution_args = {
        "datasetGroupArn": dataset_group_arn,
        "name": f"{dataset_group_name}_popular_sol",
        "performHPO": False,
        "performAutoML": False,
        "recipeArn": "arn:aws:personalize:::recipe/aws-popularity-count",
        "performAutoTraining": True,
        "solutionConfig": {
            "autoTrainingConfig": {
                "schedulingExpression": "rate(3 days)"
            },
            "trainingDataConfig": {
                "excludedDatasetColumns": { 
                    "Items": ["DAYS_SINCE_MERCHANDISABLE"]
                 }
            }
        }
    }

# COMMAND ----------

if solution == "sims":
    create_solution_args = {
        "datasetGroupArn": dataset_group_arn,
        "performHPO": True,
        "performAutoML": False,
        "performAutoTraining": True,
        "recipeArn": "arn:aws:personalize:::recipe/aws-sims",
        "eventType": "purchase",
        "name": f"{dataset_group_name}_sims_sol",
        "solutionConfig": {
            "autoTrainingConfig": {
                "schedulingExpression": "rate(3 days)"
            },
            "hpoConfig": {
                "algorithmHyperParameterRanges": {
                    "categoricalHyperParameterRanges": [],
                    "continuousHyperParameterRanges": [
                        {
                            "maxValue": 0.6,
                            "minValue": 0.4,
                            "name": "popularity_discount_factor"
                        }
                    ],
                    "integerHyperParameterRanges": [
                        {
                            "maxValue": 10,
                            "minValue": 8,
                            "name": "min_cointeraction_count"
                        }
                    ]
                },
                "hpoResourceConfig": {
                    "maxNumberOfTrainingJobs": "6",
                    "maxParallelTrainingJobs": "2"
                }
            },
            "trainingDataConfig": {
                "excludedDatasetColumns": { 
                    "Items": ["DAYS_SINCE_MERCHANDISABLE"]
                 }
            }
        }
    }

# COMMAND ----------

if solution == "up":
    create_solution_args = {
        "datasetGroupArn": dataset_group_arn,
        "name": f"{dataset_group_name}_up_sol",
        "performHPO": True,
        "performAutoML": False,
        "performAutoTraining": True,
        "recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
        "solutionConfig": {
            "autoTrainingConfig": {
                "schedulingExpression": "rate(21 days)"
            },
            "hpoConfig": {
                "algorithmHyperParameterRanges": {
                    "categoricalHyperParameterRanges": [
                        {
                            "name": "recency_mask",
                            "values": [
                                "true",
                                "false"
                            ]
                        }
                    ],
                    "continuousHyperParameterRanges": [],
                    "integerHyperParameterRanges": [
                        {
                            "maxValue": 256,
                            "minValue": 200,
                            "name": "hidden_dimension"
                        },
                        {
                            "maxValue": 32,
                            "minValue": 12,
                            "name": "bptt"
                        }
                    ]
                },
                "hpoResourceConfig": {
                    "maxNumberOfTrainingJobs": "6",
                    "maxParallelTrainingJobs": "2"
                }
            },
            "trainingDataConfig": {
                "excludedDatasetColumns": { 
                    "Items": ["DAYS_SINCE_MERCHANDISABLE"]
                 }
            }
        }
    }

# COMMAND ----------

solution_arn = personalize.create_solution(**create_solution_args)["solutionArn"]

# COMMAND ----------

status = None
while status != "ACTIVE":
    status = personalize.describe_solution(solutionArn=solution_arn)["solution"]["status"]
    if status == "CREATE FAILED":
        raise Exception(f"Error while creating {solution_arn}")
    sleep(30)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="solution_arn", value=solution_arn)
