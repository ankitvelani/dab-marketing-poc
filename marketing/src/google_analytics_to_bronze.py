# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./abc_framework/control

# COMMAND ----------

# MAGIC %run ./abc_framework/audit

# COMMAND ----------

# MAGIC %run ./framework/autoloader

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

config = get_active_config(spark, catalog, 'GoogleAnalytics')

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
job_id = context.jobId().get() if context.jobId().isDefined() else None
notebook_path = context.notebookPath().get()
pipeline_id = f"{config.source_system}{config.source_dataset}{config.target_table}"
try:
    run_id = context.currentRunId().get()
except:
    run_id = None

audit_id = start_run(spark, catalog, pipeline_id, job_id, run_id, notebook_path)

# COMMAND ----------

try:
    source_name = config.source_system
    source_dataset = config.source_dataset
    target_table = config.target_table
    source_object_path = config.source_object_path
    source_file_format = config.source_file_format
    schema_location =f"/Volumes/{catalog}/{schema}/{volume}/schema/"+target_table
    checkpoint_location = f"/Volumes/{catalog}/{schema}/{volume}/checkpoint/"+target_table

    res = ingest_to_bronze(spark, source_name, source_dataset, source_object_path, schema_location, checkpoint_location, target_table, source_file_format, True)


    rows_written = res['rows_written'],
    files_processed = res['files_processed']
    status = "SUCCESS" 
    batch_id = res['batch_id']
    error_message = ""

    end_run(spark, catalog, audit_id, status, rows_written, files_processed, batch_id, error_message)

except Exception as e:
    end_run(spark, catalog, audit_id, status="FAILED", error_message=str(e))
    raise