# Databricks notebook source
# MAGIC %pip install boto3 --upgrade

# COMMAND ----------

from time import sleep

dbutils.widgets.text("solution", "")
solution = dbutils.widgets.get("solution")

if solution not in ("sims", "popular", "up"):
    raise Exception(f"Invalid solution {solution}")

print(f"""
solution: {solution}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
personalize = utils.get_personalize_client(temp_creds)

# COMMAND ----------

solution_arn = dbutils.jobs.taskValues.get(taskKey=f"create_solution_{solution}", key="solution_arn")
solution_version_arn = personalize.create_solution_version(
    solutionArn=solution_arn,
    trainingMode="FULL"
)["solutionVersionArn"]

# COMMAND ----------

status = None
while status != "ACTIVE":
    temp_creds = utils.get_temp_aws_credentials(duration=20*60)
    personalize = utils.get_personalize_client(temp_creds)
    status = personalize.describe_solution_version(
        solutionVersionArn=solution_version_arn
    )["solutionVersion"]["status"]
    if status == "CREATE FAILED":
        raise Exception(f"Error while creating {solution_version_arn}")
    sleep(600)

# COMMAND ----------

dbutils.jobs.taskValues.set(key=f"{solution}_solution_version_arn", value=solution_version_arn)
