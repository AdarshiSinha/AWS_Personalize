# Databricks notebook source
import requests

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("dataset_group_name", "")
dataset_group_name = dbutils.widgets.get("dataset_group_name")
if not dataset_group_name:
    dataset_group_name = dbutils.jobs.taskValues.get(taskKey="create_dataset_group", key="dataset_group_name")

dbutils.widgets.text("poet_model_version", "")
poet_model_version = dbutils.widgets.get("poet_model_version")
if not poet_model_version:
    raise ValueError("poet_model_version is not set")

print(f"""
environment: {environment}
dataset_group_name: {dataset_group_name}
poet_model_version: {poet_model_version}
""")

# COMMAND ----------

# MAGIC %run ../recs_utils

# COMMAND ----------

region, model_version, version = dataset_group_name.split("_")
utils = Utils(spark, dbutils)

anonymous_token = requests.request(
    method="POST",
    url=dbutils.secrets.get(scope="personalizationProductRecommender", key="tempTokenUrl"),
    headers={},
    data={}
).json()["anonymousToken"]

def get_recommended_items(model_id, poet_model_version, locale, count=10, mpv_id="", page_section=""):
    endpoint_env = "" if environment == "prd" else "-dev"
    item_param = f"&itemId={mpv_id}" if ("sims" in model_id or "category" in model_id) else ""
    score_path = "category/score" if "category" in model_id else "score"
    url = (f"https://recommendations{endpoint_env}.dna.vpsvc.com/v1/{score_path}"
           f"?modelVersion={poet_model_version}"
           f"&count={count}"
           f"&context=%7B%22locale%22%3A%22{locale}%22%2C%22page_section%22%3A%22{page_section}%22%7D"
           f"&userId=non-existing-user"
           f"&modelId={model_id}"
           f"&locale={utils.region_code[locale]}"
           f"{item_param}")

    print(f"Testing URL: {url}")

    return requests.request(
        method="GET",
        url=url,
        headers={"Authorization": f"Bearer {anonymous_token}"},
        data={}
    ).json()

# COMMAND ----------

# SIMS correct logic test
for locale in utils.region_locale[region]:
    response = get_recommended_items(f"{version}_sims", poet_model_version, locale, mpv_id="wallCalendars")
    print(response)
    assert "deskCalendars" in [item["itemId"] for item in response["itemList"]]

# COMMAND ----------

# SIMS accessory filter test
mpv_ids = {
    "us": "christmasCards",
    "dach": "holidayCards",
    "uk": "ukieChristmasCard",
}
mpv_id = mpv_ids.get(region)
if mpv_id:
    for locale in utils.region_locale[region]:
        response = get_recommended_items(f"{version}_sims", poet_model_version, locale, mpv_id=mpv_id)
        print(response)
        assert "whiteEnvelopes" not in [item["itemId"] for item in response["itemList"]]

# COMMAND ----------

# Most popular sanity test
for locale in utils.region_locale[region]:
    response = get_recommended_items(f"{version}_most_popular", poet_model_version, locale)
    print(response)
    assert "standardBusinessCards" in [item["itemId"] for item in response["itemList"]]

# COMMAND ----------

# UP new user test
for locale in utils.region_locale[region]:
    response = get_recommended_items(f"{version}_user_personalization", poet_model_version, locale)
    print(response)
    assert "standardBusinessCards" in [item["itemId"] for item in response["itemList"]]

# COMMAND ----------

# UP category filter test
for locale in utils.region_locale[region]:
    response = get_recommended_items(
        f"{version}_user_personalization_category_filter",
        poet_model_version,
        locale,
        count=50,
        mpv_id="standardBusinessCards")
    print(response)
    response_items = set([item["itemId"] for item in response["itemList"]])
    expected_items = {"roundedCornerBusinessCards", "squareBusinessCards", "glossyBusinessCards"}
    assert expected_items.issubset(response_items)

# COMMAND ----------

# UP logomaker filter test - logomaker filter produce different results
for locale in utils.region_locale[region]:
    lm_response = get_recommended_items(
        f"{version}_user_personalization_logomaker_filter",
        poet_model_version,
        locale,
        count=50)
    print(lm_response)
    up_response = get_recommended_items(
        f"{version}_user_personalization",
        poet_model_version,
        locale,
        count=50)
    print(up_response)
    lm_response_items = set([item["itemId"] for item in lm_response["itemList"]])
    up_response_items = set([item["itemId"] for item in up_response["itemList"]])
    assert lm_response_items != up_response_items

# COMMAND ----------

# UP locale in context test - different locales produce different results
locales = {
    "frcen": ["FR", "NL"],
    "anzs":  ["AU", "NZ"],
    "dach":  ["DE", "CH"],
    "eu":    ["ES", "DK"],
    "uk":    ["GB", "IE"],
}
if region in locales:
    combined_result = []
    for locale in locales[region]:
        response = get_recommended_items(f"{version}_user_personalization", poet_model_version, locale, count=50)
        print(response)
        combined_result.append([item["itemId"] for item in response["itemList"]])
    assert combined_result[0] != combined_result[1]

# COMMAND ----------

# UP page section test - different page sections produce different results
for locale in utils.region_locale[region]:
    hp_response = get_recommended_items(
        f"{version}_user_personalization",
        poet_model_version,
        locale,
        count=50,
        page_section="Home Page")
    print(hp_response)
    cr_response = get_recommended_items(
        f"{version}_user_personalization",
        poet_model_version,
        locale,
        count=50,
        page_section="Configure-Recommendation")
    print(cr_response)
    hp_response_items = [item["itemId"] for item in hp_response["itemList"]]
    cr_response_items = [item["itemId"] for item in cr_response["itemList"]]
    assert hp_response_items != cr_response_items
