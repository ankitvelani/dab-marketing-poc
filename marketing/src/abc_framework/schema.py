# Databricks notebook source
# Set default catalog and schema
# catalog = dbutils.widgets.get("catalog")
catalog = "dev_marketing_intelligence"
schema = "control_framework"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# DBTITLE 1,pipeline config
spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pipeline_config (
    source_system             STRING NOT NULL,    -- google_ads, facebook_ads, ga4
    source_dataset            STRING NOT NULL,    -- campaign, keyword, events
    source_object_path        STRING NOT NULL,   -- file path or source table
    pipeline_layer            STRING NOT NULL,    -- BRONZE, SILVER, GOLD
    target_table              STRING NOT NULL,    -- Source file format (primarily for Bronze)
    source_file_format        STRING,             -- csv, json, parquet
    balance_type              STRING NOT NULL,    -- ROW_COUNT, DISTINCT_COUNT, METRIC_SUM, NONE
    balance_column            STRING,             -- campaign_id, adgroup_id, spend, revenue
    balance_filter_column     STRING,             -- source_system
    balance_threshold_pct     DOUBLE NOT NULL,
    is_active                 BOOLEAN NOT NULL,
    last_successful_load_ts   TIMESTAMP,
    updated_ts                TIMESTAMP) USING DELTA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Config Entry

# COMMAND ----------

# DBTITLE 1,Cell 3
### Use Merge into statement to insert unique records ....No primary key to enforce it must be combination 
# MERGE INTO control.pipeline_config t
# USING source_data s
# ON  t.source_system  = s.source_system
# AND t.source_dataset = s.source_dataset
# AND t.pipeline_layer = s.pipeline_layer
# AND t.target_table   = s.target_table

# WHEN NOT MATCHED THEN
# INSERT (...);

def insert_pipeline_config(
    source_system: str,
    source_dataset: str,
    source_object_path: str,
    pipeline_layer: str,
    target_table: str,
    source_file_format: str,
    balance_type: str,
    balance_column: str,
    balance_filter_column: str,
    balance_threshold_pct: float,
    is_active: bool
) -> None:
    """
    Insert or update pipeline configuration using MERGE statement.
    
    Args:
        source_system: Source system identifier (e.g., 'google_ads', 'facebook_ads', 'ga4')
        source_dataset: Dataset name (e.g., 'campaign', 'keyword', 'events')
        pipeline_layer: Pipeline layer identifier ('BRONZE', 'SILVER', 'GOLD')
        source_object_path: Source file path or table name
        target_table: Target table name
        source_file_format: File format ('csv', 'json', 'parquet')
        balance_type: Balance validation type ('ROW_COUNT', 'DISTINCT_COUNT', 'METRIC_SUM', 'NONE')
        balance_column: Column name for balance validation
        balance_filter_column: Column name for filtering during balance validation
        balance_threshold_pct: Threshold percentage for balance validation
        is_active: Whether the pipeline configuration is active
    
    Raises:
        ValueError: If required parameters are invalid
        Exception: If database operation fails
    """
    # Input validation
    if not all([source_system, source_dataset, pipeline_layer, target_table]):
        raise ValueError("source_system, source_dataset, pipeline_layer, and target_table are required")
    
    valid_layers = ['BRONZE', 'SILVER', 'GOLD']
    if pipeline_layer not in valid_layers:
        raise ValueError(f"pipeline_layer must be one of {valid_layers}")
    
    valid_balance_types = ['ROW_COUNT', 'DISTINCT_COUNT', 'METRIC_SUM', 'NONE']
    if balance_type not in valid_balance_types:
        raise ValueError(f"balance_type must be one of {valid_balance_types}")
    
    if not 0 <= balance_threshold_pct <= 100:
        raise ValueError("balance_threshold_pct must be between 0 and 100")
    
    try:
        # Use parameterized query to prevent SQL injection
        spark.sql("""
            MERGE INTO {catalog}.{schema}.pipeline_config AS t
            USING (
                SELECT 
                    :source_system AS source_system,
                    :source_dataset AS source_dataset,
                    :source_object_path AS source_object_path,
                    :pipeline_layer AS pipeline_layer,
                    :target_table AS target_table,
                    :source_file_format AS source_file_format,
                    :balance_type AS balance_type,
                    :balance_column AS balance_column,
                    :balance_filter_column AS balance_filter_column,
                    :balance_threshold_pct AS balance_threshold_pct,
                    :is_active AS is_active,
                    NULL AS last_successful_load_ts,
                    current_timestamp() AS updated_ts
            ) AS s
            ON  t.source_system = s.source_system
            AND t.source_dataset = s.source_dataset
            AND t.pipeline_layer = s.pipeline_layer
            AND t.target_table = s.target_table
            WHEN NOT MATCHED THEN
                INSERT (
                    source_system,
                    source_dataset,
                    source_object_path,
                    pipeline_layer,
                    target_table,
                    source_file_format,
                    balance_type,
                    balance_column,
                    balance_filter_column,
                    balance_threshold_pct,
                    is_active,
                    last_successful_load_ts,
                    updated_ts
                )
                VALUES (
                    s.source_system,
                    s.source_dataset,
                    s.source_object_path,
                    s.pipeline_layer,
                    s.target_table,
                    s.source_file_format,
                    s.balance_type,
                    s.balance_column,
                    s.balance_filter_column,
                    s.balance_threshold_pct,
                    s.is_active,
                    s.last_successful_load_ts,
                    s.updated_ts
                )
        """.format(catalog=catalog, schema=schema), {
            "source_system": source_system,
            "source_dataset": source_dataset,
            "source_object_path": source_object_path,
            "pipeline_layer": pipeline_layer,
            "target_table": target_table,
            "source_file_format": source_file_format,
            "balance_type": balance_type,
            "balance_column": balance_column,
            "balance_filter_column": balance_filter_column,
            "balance_threshold_pct": balance_threshold_pct,
            "is_active": is_active
        })
        
        print(f"Successfully merged configuration for {source_system}.{source_dataset} -> {target_table}")
        
    except Exception as e:
        print(f"Error inserting pipeline config: {str(e)}")
        raise

# COMMAND ----------

insert_pipeline_config(source_system="GOOGLE_ANALYTICS",
                       source_dataset="GoogleAnalytics",
                       source_object_path="/Volumes/source/raw/datasets/GoogleAnalytics/",
                       pipeline_layer="BRONZE",
                       target_table="google_analytics_events",
                       source_file_format="csv",
                       balance_type="ROW_COUNT",
                       balance_column=None,
                       balance_filter_column=None,
                       balance_threshold_pct=0,
                       is_active=True
                       )

# COMMAND ----------

# DBTITLE 1,Audit log
spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_log (
    audit_id              STRING NOT NULL,      -- UUID
    pipeline_config_id    STRING NOT NULL,      --combination of source_system, source_dataset, pipeline_layer, target_table
    job_id                STRING, -- Databricks Execution Context
    job_name              STRING,
    run_id                STRING,
    notebook_path         STRING,
    batch_id              STRING,     -- Batch Information
    start_ts              TIMESTAMP NOT NULL,
    end_ts                TIMESTAMP,
    status                STRING NOT NULL,      -- STARTED, SUCCESS, FAILED
    files_processed       BIGINT,
    rows_read             BIGINT,
    rows_written          BIGINT,
    error_message         STRING,
    created_ts            TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (pipeline_config_id)""")

# COMMAND ----------

# DBTITLE 1,Balance Log
spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.balance_log (
    balance_id          STRING NOT NULL,      -- UUID
    audit_id            STRING NOT NULL,      -- FK to audit_log
    source_value        DOUBLE,
    target_value        DOUBLE,
    variance_pct        DOUBLE,
    threshold_pct       DOUBLE,
    passed              BOOLEAN,
    details             STRING,
    created_ts          TIMESTAMP NOT NULL
)
USING DELTA
""")