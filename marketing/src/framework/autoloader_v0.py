# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

def source_to_bronze(source_system,
                     file_format,
                     schema_location,
                     landing_path,
                     checkpoint_location,
                     bronze_table):
    """
    """
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", file_format)\
        .option("cloudFiles.schemaLocation", schema_location)\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .option("cloudFiles.inferColumnTypes", "true")\
        .option("cloudFiles.maxFilesPerTrigger", "1000")\
        .option("cloudFiles.validateOptions", "true")\
        .load(landing_path)\
        .withColumn("_source_system", F.lit(source_system))\
        .withColumn("_ingestion_ts", F.current_timestamp())\
        .withColumn("_source_file", F.col("_metadata.file_path"))


    df.writeStream\
        .format("delta")\
        .option("checkpointLocation", checkpoint_location)\
        .option("mergeSchema", "true")\
        .trigger(availableNow=True)\
        .outputMode("append")\
        .toTable(bronze_table)