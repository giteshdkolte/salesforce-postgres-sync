# utils/rds_to_sf_logger.py

import psycopg2
from datetime import datetime
import logging

logger = logging.getLogger("data_migration_pipeline")

def _conn(conn_str: str):
    return psycopg2.connect(conn_str)

# ── (1) God Summary Log Table ─────────────────────────────────────────────
SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.pipeline_sf_load_summary (
    id                  SERIAL PRIMARY KEY,
    run_id              VARCHAR(64),
    rds_table_source    VARCHAR(255),
    salesforce_object   VARCHAR(255),
    method              VARCHAR(20),
    external_id_field   VARCHAR(255),
    total_rows          BIGINT,
    success_count       BIGINT,
    failed_count        BIGINT,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    duration_seconds    NUMERIC(10, 2),
    status              VARCHAR(20),
    error_summary       TEXT
);
"""

# ── (2) Row-Level Detail Table (per rds_table) ────────────────────────────
ROW_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{row_table} (
    id                  SERIAL PRIMARY KEY,
    run_id              VARCHAR(64),
    salesforce_object   VARCHAR(255),
    method              VARCHAR(20),
    batch_num           INT,
    old_id              VARCHAR(255),
    new_id              VARCHAR(255),
    status              VARCHAR(20),
    error_message       TEXT,
    processed_at        TIMESTAMP
);
"""

def ensure_log_tables(conn_str: str, schema: str, rds_table: str) -> str:
    """
    Ensures both the summary and row-level tables exist.
    Returns the row-level table name for reuse.
    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS.
    """
    row_table = f"{rds_table}_level_row_information"
    try:
        conn = _conn(conn_str)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SUMMARY_DDL.format(schema=schema))
            cur.execute(ROW_DDL.format(schema=schema, row_table=row_table))
        conn.close()
        logger.debug(f"Log tables ensured in schema: {schema}")
    except Exception as e:
        logger.error(f"Log table creation failed in '{schema}': {type(e).__name__}: {e}")
    return row_table

def log_summary(
    conn_str: str, schema: str, run_id: str,
    rds_table: str, sf_object: str, method: str, ext_id_field: str,
    total: int, success: int, failed: int,
    started_at: datetime, completed_at: datetime,
    error_summary: str = None
):
    """
    Writes one summary row to pipeline_sf_load_summary.
    Called once per job_key after all batches complete.
    Opens its own short-lived connection — called only once per object.
    """
    duration = round((completed_at - started_at).total_seconds(), 2)
    status   = "SUCCESS" if failed == 0 else ("FAILED" if success == 0 else "PARTIAL")
    sql = f"""
        INSERT INTO {schema}.pipeline_sf_load_summary
        (run_id, rds_table_source, salesforce_object, method, external_id_field,
         total_rows, success_count, failed_count, started_at, completed_at,
         duration_seconds, status, error_summary)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    try:
        conn = _conn(conn_str)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, (
                run_id, rds_table, sf_object, method, ext_id_field,
                total, success, failed, started_at, completed_at,
                duration, status, error_summary
            ))
        conn.close()
    except Exception as e:
        logger.error(f"Summary log insert failed: {type(e).__name__}: {e}")
