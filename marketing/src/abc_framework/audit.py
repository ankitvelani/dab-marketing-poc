# Databricks notebook source
import uuid
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

def start_run(spark: SparkSession, catalog, pipeline_id, job_id, run_id, notebook_path) -> str:
    """Insert a STARTED row, return the audit_id to pass through the rest of the run."""
    audit_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO {catalog}.control_framework.audit_log
        (audit_id, pipeline_config_id, job_id, run_id, notebook_path, start_ts, status,
        created_ts)
        VALUES (
            '{audit_id}', {f"'{pipeline_id}'"}, {f"'{job_id}'" if job_id else 'NULL'}, {f"'{run_id}'"}, {f"'{notebook_path}'"},
            '{datetime.utcnow().isoformat()}', 'STARTED', '{datetime.utcnow().isoformat()}'
        )
    """)
    return audit_id

# COMMAND ----------

def end_run(
    spark: SparkSession,
    catalog: str,
    audit_id: str,
    status: str,
    rows_written: int = None,
    files_processed: int = None,
    batch_id: str = None,
    error_message: str = None,
) -> None:
    """
    Update the audit row created by start_run(). status is 'SUCCESS' or 'FAILED'.
    Call this in a try/finally in the ingestion script so failures are logged too.
    """

    error_sql = f"'{error_message.replace(chr(39), chr(34))}'" if error_message else "NULL"
    batch_sql = f"'{batch_id}'" if batch_id else "NULL"
    # Extract integer from tuple if needed (Cell 10 has trailing comma bug)
    rows_written_val = rows_written[0] if isinstance(rows_written, tuple) else rows_written
    spark.sql(f"""
        UPDATE {catalog}.control_framework.audit_log
        SET end_ts = '{datetime.utcnow().isoformat()}',
            status = '{status}',
            rows_written = {rows_written_val if rows_written_val is not None else 'NULL'},
            files_processed = {files_processed if files_processed is not None else 'NULL'},
            batch_id = {batch_sql},
            error_message = {error_sql}
        WHERE audit_id = '{audit_id}'
    """)

# COMMAND ----------

