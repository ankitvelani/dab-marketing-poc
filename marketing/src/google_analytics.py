# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

# COMMAND ----------

# DBTITLE 1,Cell 4
source_system = "GOOGLE_ANALYTICS"
dataset_name ="GoogleAnalytics"
file_format = "csv"
schema_location =f"/Volumes/{catalog}/{schema}/{volume}/schema/"+dataset_name
landing_path = "/Volumes/source/raw/datasets/"+dataset_name
checkpoint_location = f"/Volumes/{catalog}/{schema}/{volume}/checkpoint/"+dataset_name
bronze_table = f"{catalog}.{schema}.{dataset_name}"

source_to_bronze(source_system, file_format, schema_location, landing_path, checkpoint_location, bronze_table)