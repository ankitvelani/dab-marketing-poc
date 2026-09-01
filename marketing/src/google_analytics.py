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

# COMMAND ----------

source_system = "GOOGLE_ANALYTICS"
dataset_name ="GoogleAnalytics"
file_format = "csv"
schema_location = "/Volumes/source/raw/schema/"+dataset_name
landing_path = "/Volumes/source/raw/datasets/"+dataset_name
checkpoint_location = "/Volumes/source/raw/checkpoint/"+dataset_name
bronze_table = "{catalog}.{schema}."+dataset_name

source_to_bronze(source_system, file_format, schema_location, landing_path, checkpoint_location, bronze_table)