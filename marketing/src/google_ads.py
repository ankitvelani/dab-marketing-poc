# Databricks notebook source
# MAGIC %run ../src/framework/autoloader

# COMMAND ----------

GOOGLE_ADS_ENTITIES = ["google_ads__account_report",
                        "google_ads__campaign_report",
                        "google_ads__ad_group_report",
                        "google_ads__keyword_report",
                        "google_ads__ad_report",
                        "google_ads__search_term_report",
                        "google_ads__url_report"]

def build_entity_configs(dataset_name, entities: list[str], landing_root: str, catalog_schema: str):
    return [
       {
        "source_system": f"{dataset_name}_{e}",
        "file_format": "csv",
        "schema_location": f"/Volumes/source/raw/schema/{dataset_name}/{e}",
        "landing_path": f"{landing_root}/{dataset_name}/{e}/*",
        "checkpoint_location": f"/Volumes/source/raw/checkpoint/{dataset_name}/{e}",
        "bronze_table": f"{catalog_schema}.{e}"
        }
        for e in entities
    ]

configs  = build_entity_configs("Google_Ads", GOOGLE_ADS_ENTITIES, "/Volumes/source/raw/datasets/", "dev_marketing_intelligence.dev")
for cfg in configs:
    source_to_bronze(**cfg)