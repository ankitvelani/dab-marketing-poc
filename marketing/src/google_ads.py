# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../src/framework/autoloader

# COMMAND ----------

# DBTITLE 1,Cell 2
# Set default catalog and schema
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
entity = dbutils.widgets.get("entity")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")



# Create volume if not exists
volume = "data_storage"
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

# COMMAND ----------

dataset_name = "Google_Ads"
landing_root = "/Volumes/source/raw/datasets/"

source_to_bronze(
    source_system=entity,
    file_format="csv",
    schema_location=f"/Volumes/{catalog}/{schema}/{volume}/schema/{dataset_name}/{entity}",
    landing_path=f"{landing_root}{dataset_name}/{entity}",
    checkpoint_location=f"/Volumes/{catalog}/{schema}/{volume}/checkpoint/{dataset_name}/{entity}",
    bronze_table=f"{catalog}.{schema}.{entity}",
)