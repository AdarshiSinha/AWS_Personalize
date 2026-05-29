# Databricks notebook source
import json

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("dataset_group_name", "")
dataset_group_name = dbutils.widgets.get("dataset_group_name")
if not dataset_group_name:
    dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

dbutils.widgets.text("event_tracker_arn", "")
event_tracker_arn = dbutils.widgets.get("event_tracker_arn")
if not event_tracker_arn:
    event_tracker_arn = dbutils.jobs.taskValues.get(taskKey="create_event_tracker", key="event_tracker_arn")

dbutils.widgets.text("dataset_group_arn", "")
dataset_group_arn = dbutils.widgets.get("dataset_group_arn")
if not dataset_group_arn:
    dataset_group_arn = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_arn")

dbutils.widgets.text("interactions_dataset_arn", "")
interactions_dataset_arn = dbutils.widgets.get("interactions_dataset_arn")
if not interactions_dataset_arn:
    interactions_dataset_arn = dbutils.jobs.taskValues.get(taskKey="create_dataset_interactions", key="dataset_arn")

dbutils.widgets.text("up_campaign_arn", "")
up_campaign_arn = dbutils.widgets.get("up_campaign_arn")
if not up_campaign_arn:
    up_campaign_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_up", key="up_sol_cpn_arn")

dbutils.widgets.text("sims_campaign_arn", "")
sims_campaign_arn = dbutils.widgets.get("sims_campaign_arn")
if not sims_campaign_arn:
    sims_campaign_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_sims", key="sims_sol_cpn_arn")

dbutils.widgets.text("popular_campaign_arn", "")
popular_campaign_arn = dbutils.widgets.get("popular_campaign_arn")
if not popular_campaign_arn:
    popular_campaign_arn = dbutils.jobs.taskValues.get(taskKey="create_campaign_popular", key="popular_sol_cpn_arn")

print(f"""
environment: {environment}
dataset_group_name: {dataset_group_name}
dataset_group_arn: {dataset_group_arn}
event_tracker_arn: {event_tracker_arn}
interactions_dataset_arn: {interactions_dataset_arn}
up_campaign_arn: {up_campaign_arn}
sims_campaign_arn: {sims_campaign_arn}
popular_campaign_arn: {popular_campaign_arn}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

utils = Utils(spark, dbutils)
temp_creds = utils.get_temp_aws_credentials()
aws_session = utils.get_temp_session(temp_creds)
client = aws_session.client('cloudwatch', region_name='eu-west-1')
sns_topic_arn = "arn:aws:sns:eu-west-1:905666450942:relcore_recommendations_alerts"
env_indicator = "prd" if environment == "prd" else "dev"
region = dataset_group_name.split("_")[0]

# COMMAND ----------

dashboard_body = {
    "widgets": [
        {
            "height": 7,
            "width": 12,
            "y": 0,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [
                        "AWS/Personalize",
                        "putEventsRequests",
                        "EventTrackerArn", event_tracker_arn,
                        "DatasetArn", interactions_dataset_arn,
                        "DatasetGroupArn", dataset_group_arn,
                        {
                            "region": "eu-west-1",
                            "label": "Events received"
                        }
                    ]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": "eu-west-1",
                "stat": "Sum",
                "period": 300,
                "title": "Events received"
            }
        },
        {
            "height": 7,
            "width": 12,
            "y": 0,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [
                        "AWS/Personalize",
                        "GetRecommendations",
                        "CampaignArn", up_campaign_arn,
                        {
                            "region": "eu-west-1",
                            "label": "up"
                        }
                    ],
                    [
                        "AWS/Personalize",
                        "GetRecommendations",
                        "CampaignArn", sims_campaign_arn,
                        {
                            "region": "eu-west-1",
                            "label": "sims"
                        }
                    ],
                    [
                        "AWS/Personalize",
                        "GetRecommendations",
                        "CampaignArn", popular_campaign_arn,
                        {
                            "region": "eu-west-1",
                            "label": "popular"
                        }
                    ]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": "eu-west-1",
                "stat": "Sum",
                "period": 300,
                "title": "Recommendations by Model"
            }
        },
        {
            "height": 7,
            "width": 12,
            "y": 7,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [
                        "PersonalizeMonitor",
                        "averageTPS",
                        "CampaignArn", up_campaign_arn,
                        {
                            "region": "eu-west-1",
                            "label": "UP Average TPS"
                        }
                    ]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": "eu-west-1",
                "stat": "Average",
                "period": 300,
                "title": "UP Average TPS"
            }
        },
        {
            "height": 7,
            "width": 12,
            "y": 7,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [
                        "AWS/Personalize",
                        "GetRecommendationsLatency",
                        "CampaignArn", up_campaign_arn,
                        {
                            "region": "eu-west-1",
                            "label": "UP Average Latency"
                        }
                    ]
                ],
                "view": "timeSeries",
                "stacked": False,
                "region": "eu-west-1",
                "stat": "Average",
                "period": 300,
                "title": "UP Average Latency"
            }
        }
    ]
}

client.put_dashboard(
    DashboardName=f"{dataset_group_name}_monitoring_{env_indicator}",
    DashboardBody=json.dumps(dashboard_body)
)

# COMMAND ----------

configs = {
    "us": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600 * 3},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "ca": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600 * 3},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "anzs": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600 * 12},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "eu": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "frcen": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "in": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600 * 24},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 24},
    },
    "dach": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
    "uk": {
        "UP": {"arn": up_campaign_arn, "period_seconds": 60},
        "MP": {"arn": popular_campaign_arn, "period_seconds": 3600},
        "SIMS": {"arn": sims_campaign_arn, "period_seconds": 3600 * 6},
    },
}

for campaign_name, config in configs[region].items():
    client.put_metric_alarm(
    AlarmName=f"[{env_indicator.upper()}] {dataset_group_name} - idle {campaign_name} campaign",
    ActionsEnabled=False,
    AlarmActions=[sns_topic_arn],
    InsufficientDataActions=[sns_topic_arn],
    MetricName="GetRecommendations",
    Namespace="AWS/Personalize",
    Statistic="Sum",
    Dimensions=[
        {
            "Name": "CampaignArn",
            "Value": config["arn"]
        }
    ],
    Period=config["period_seconds"],
    EvaluationPeriods=1,
    DatapointsToAlarm=1,
    Threshold=0,
    ComparisonOperator="LessThanOrEqualToThreshold",
)

# COMMAND ----------

client.put_metric_alarm(
    AlarmName=f"[{env_indicator.upper()}] {dataset_group_name} - idle event tracker",
    ActionsEnabled=True if environment == "prd" else False,
    AlarmActions=[sns_topic_arn],
    InsufficientDataActions=[sns_topic_arn],
    MetricName="putEventsRequests",
    Namespace="AWS/Personalize",
    Statistic="Sum",
    Dimensions=[
        {
            "Name": "DatasetGroupArn",
            "Value": dataset_group_arn
        },
        {
            "Name": "DatasetArn",
            "Value": interactions_dataset_arn
        },
        {
            "Name": "EventTrackerArn",
            "Value": event_tracker_arn
        }
    ],
    Period=300,
    EvaluationPeriods=1,
    DatapointsToAlarm=1,
    Threshold=0,
    ComparisonOperator="LessThanOrEqualToThreshold",
)
