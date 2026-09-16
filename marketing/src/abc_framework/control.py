# Databricks notebook source
"""
Control: config-driven run eligibility.
Every ingestion script asks this module "should I run, and with what
parameters?" instead of hardcoding paths/flags. Also owns writing back
last_successful_load_ts once a run completes cleanly.
"""

from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

@dataclass
class SourceConfig:
    source_system:str
    source_dataset: str
    source_object_path: str
    target_table: str
    source_file_format: str
    is_active: bool
    balance_threshold_pct: float

def get_active_config(spark, catalog, source_dataset):
    row = (
        spark.table(f"{catalog}.control_framework.pipeline_config")
        .filter(f"source_dataset = '{source_dataset}'")
        .collect()
    )

    if not row:
        raise ValueError(f"No pipeline_config entry for source '{source_dataset}'")

    row = row[0]
    if not row["is_active"]:
        raise RuntimeError(
            f"Source '{source_dataset}' is marked is_active=false in pipeline_config. "
            f"Enable it there before running this job."
        )

    return SourceConfig(
            source_system= row['source_system'],
            source_dataset= row['source_dataset'],
            target_table= row['target_table'],
            source_object_path= row['source_object_path'],
            source_file_format= row['source_file_format'],
            balance_threshold_pct= row['balance_threshold_pct'],
            is_active=row["is_active"]
    )
