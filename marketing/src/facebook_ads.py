# Databricks notebook source
# MAGIC %run ../src/framework/autoloader

# COMMAND ----------

# Set default catalog and schema
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

# Create volume if not exists
volume = "data_storage"
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.volume")

# COMMAND ----------

FACEBOOK_ADS_ENTITIES = ["facebook_ads__account_report",
                        "facebook_ads__campaign_report",
                        "facebook_ads__ad_set_report",
                        "facebook_ads__ad_report",
                        "facebook_ads__country_report"]

def build_entity_configs(dataset_name, entities: list[str], landing_root: str, catalog_schema: str):
    return [
       {
        "source_system": f"{dataset_name}_{e}",
        "file_format": "csv",
        "schema_location": f"/Volumes/{catalog}/{schema}/{volume}/schema/{dataset_name}/{e}",
        "landing_path": f"{landing_root}/{dataset_name}/{e}/*",
        "checkpoint_location": f"/Volumes/{catalog}/{schema}/{volume}/checkpoint/{dataset_name}/{e}",
        "bronze_table": f"{catalog_schema}.{e}"
        }
        for e in entities
    ]

configs  = build_entity_configs("Facebook", FACEBOOK_ADS_ENTITIES, "/Volumes/source/raw/datasets/", f"{catalog}.{schema}")
for cfg in configs:
    source_to_bronze(**cfg)