# rds_to_sf_pipeline.py
# RDS (Postgres) -> Salesforce Pipeline
# Supports: insert / update / upsert
# Memory efficient: server-side cursor, persistent log connection, batch gc
# Large volume: groups MAX_BATCHES_PER_JOB partitions into one SF Bulk API Job

import logging
import uuid
import gc
import yaml
from datetime import datetime
import sqlalchemy as sa
from simple_salesforce import Salesforce
import psycopg2
from psycopg2.extras import execute_values

from utils.dot_env_secrets import load_env_as_dict
from utils.rds_to_sf_logger import ensure_log_tables, log_summary

logger = logging.getLogger("data_migration_pipeline")

# ── Global defaults ───────────────────────────────────────────────────────
# batch_size        : rows per RDS partition + SF internal batch size
# MAX_BATCHES_PER_JOB: partitions grouped into one SF Bulk API Job
#                      10 × 1000 = 10,000 rows per job
#                      safely under SF hard limit of 15 batches per job
DEFAULT_BATCH_SIZE  = 1000
MAX_BATCHES_PER_JOB = 10

def get_sf_connection(
    instance_url: str,
    consumer_key: str,
    consumer_secret: str,
    domain: str
) -> Salesforce:
    """Create Salesforce connection"""
    return Salesforce(
        instance_url    = instance_url,
        consumer_key    = consumer_key,
        consumer_secret = consumer_secret,
        domain          = domain
    )

def prepare_payload(
    records: list,
    method: str,
    id_column: str,
    ext_id_field: str = None
) -> tuple:
    """
    Prepares SF payload and extracts old_ids for logging.

    insert : strip id_column → SF auto-generates Id
    update : keep id_column as 'Id' → SF needs it to match
    upsert :
        if id_column == external_id_field → KEEP it (SF needs it to match)
        if id_column != external_id_field → STRIP it (only for logging)
    """
    payload = []
    old_ids = []

    for record in records:
        old_id = record.get(id_column)
        old_ids.append(old_id)
        row = dict(record)

        if method == "insert":
            row.pop(id_column, None)

        elif method == "update":
            if id_column.lower() != "id":
                row["Id"] = row.pop(id_column)

        elif method == "upsert":
            if id_column.lower() != (ext_id_field or "").lower():
                row.pop(id_column, None)

        payload.append(row)

    return payload, old_ids

def push_batch_to_sf(
    sf: Salesforce,
    sf_object: str,
    method: str,
    records: list,
    id_column: str,
    ext_id_field: str = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list:
    """
    Prepares and pushes one job buffer to Salesforce via Bulk API.
    SF internally splits into batches of batch_size within this one job.

    records      : accumulated RDS rows for this job (multiple partitions)
    id_column    : RDS PK column — used for logging, handled per method
    ext_id_field : required for upsert — SF field to match on
    Returns      : list of row result dicts → old_id, new_id, status, error_message
    """
    bulk_obj         = getattr(sf.bulk, sf_object)
    payload, old_ids = prepare_payload(records, method, id_column, ext_id_field)

    try:
        if method == "insert":
            results = bulk_obj.insert(payload, batch_size=batch_size, use_serial=True)
        elif method == "update":
            results = bulk_obj.update(payload, batch_size=batch_size, use_serial=True)
        elif method == "upsert":
            results = bulk_obj.upsert(payload, ext_id_field, batch_size=batch_size, use_serial=True)
        else:
            raise ValueError(f"Unsupported method: '{method}'. Use insert / update / upsert.")

    except Exception as e:
        # Whole job failed at API level — mark every row as failed
        return [{
            "old_id": old_id, "new_id": None,
            "status": "FAILED", "error_message": str(e)
        } for old_id in old_ids]

    # ── Map per-row SF results back ───────────────────────────────────────
    row_results = []
    for old_id, res in zip(old_ids, results):
        if res.get("success"):
            row_results.append({
                "old_id":        old_id,
                "new_id":        res.get("id"),
                "status":        "SUCCESS",
                "error_message": None,
            })
        else:
            errors  = res.get("errors", [])
            err_msg = "; ".join(
                f"{e.get('statusCode')}: {e.get('message')}"
                for e in errors
            ) if errors else "Unknown error"
            row_results.append({
                "old_id":        old_id,
                "new_id":        None,
                "status":        "FAILED",
                "error_message": err_msg,
            })
    return row_results

def _write_row_batch(
    pg_cur,
    schema: str,
    row_table: str,
    run_id: str,
    sf_object: str,
    method: str,
    job_num: int,
    rows: list
):
    """
    Writes row-level results using an already-open psycopg2 cursor.
    No new connection opened — reuses persistent log connection.
    Called once per SF Job flush.
    """
    if not rows:
        return
    now    = datetime.now()
    values = [
        (
            run_id, sf_object, method, job_num,
            r.get("old_id"), r.get("new_id"),
            r.get("status"), r.get("error_message"),
            now
        )
        for r in rows
    ]
    sql = f"""
        INSERT INTO {schema}.{row_table}
        (run_id, salesforce_object, method, batch_num,
         old_id, new_id, status, error_message, processed_at)
        VALUES %s;
    """
    execute_values(pg_cur, sql, values)

def run_rds_to_salesforce(objects_to_load: list = None) -> str:

    with open(f"./config/configurations.yaml") as stream:
        config        = yaml.safe_load(stream)
        secrets       = load_env_as_dict(".env")

        jobs   = config.get("target_objects")

        # Ignore the objects which are not needed
        if objects_to_load:
            objects_to_load = [obj.lower() for obj in objects_to_load]
        else:
            objects_to_load = [obj.lower() for obj in list(jobs.keys())]
        
        successful, failed, skipped = [], [], []

        for job_key, v in jobs.items():
            started_at = datetime.now()

            # ── Read config ───────────────────────────────────────────────
            schema              = v.get("rds_staging_schema_name")
            db_name             = v.get("rds_db_name")
            sf_object           = v.get("api_name")
            method              = v.get("sf_operation", "insert").lower()
            ext_id_field        = v.get("external_id_field")
            id_column           = v.get("old_id_column", "id")
            target_table        = v.get("target_table")
            batch_size          = v.get("batch_size", DEFAULT_BATCH_SIZE)
            max_batches_per_job = v.get("max_batches_per_job", MAX_BATCHES_PER_JOB)
            conn_str            = f"{secrets.get(f'pg_connection_string')}/{db_name}"

            # ── Replace {{SCHEMA}} placeholder in SQL ─────────────────────
            source_query = v.get("sql", "").strip().replace("{{SCHEMA}}", schema)

            log_pg_conn = None
            
            try:
                # ── Active status check ───────────────────────────────────
                active_status = v.get("active_status", "").upper()
                if active_status == 'N':
                    logger.info(f"Skipping '{job_key}': active_status=N")
                    skipped.append(job_key)
                    continue
                elif active_status != 'Y':
                    logger.warning(f"Skipping '{job_key}': invalid active_status='{active_status}'")
                    skipped.append(job_key)
                    continue
                
                if job_key.lower() in objects_to_load:
                    # ── Validate config ───────────────────────────────────────
                    if not source_query:
                        raise ValueError("'sql' is required in YAML config")
                    if not sf_object:
                        raise ValueError("'api_name' is required in YAML config")
                    if method == "upsert" and not ext_id_field:
                        raise ValueError("'external_id_field' is required for upsert")
                    if method not in ("insert", "update", "upsert"):
                        raise ValueError(f"Invalid sf_operation: '{method}'")

                    # ── Ensure RDS log tables ─────────────────────────────────
                    row_table = ensure_log_tables(conn_str, schema, target_table)

                    # ── Open ONE persistent log connection for all jobs ────────
                    # Reused across all batch writes — no new connection per batch
                    log_pg_conn = psycopg2.connect(conn_str)
                    log_pg_conn.autocommit = True
                    log_cur = log_pg_conn.cursor()

                    # ── SF connection ─────────────────────────────────────────
                    sf = get_sf_connection(
                        instance_url    = secrets.get(f"url"),
                        consumer_key    = secrets.get(f"client_id"),
                        consumer_secret = secrets.get(f"client_secret"),
                        domain          = secrets.get(f"sf_domain")
                    )

                    # ── SQLAlchemy engine with server-side cursor ─────────────
                    engine = sa.create_engine(
                        conn_str,
                        execution_options={"stream_results": True}
                    )

                    # ── Total count for progress display ──────────────────────
                    with engine.connect() as cnt_conn:
                        total_size = cnt_conn.execute(
                            sa.text(f"SELECT COUNT(*) FROM ({source_query}) AS _cnt")
                        ).scalar()

                    logger.info(f"{'='*60}")
                    logger.info(f"Starting RDS → SF | [{job_key}]")
                    logger.info(f"   SF Object          : {sf_object}")
                    logger.info(f"   Method             : {method.upper()}")
                    logger.info(f"   Schema             : {schema}")
                    logger.info(f"   Total rows         : {total_size:,}")
                    logger.info(f"   Batch size         : {batch_size:,}")
                    logger.info(f"   Partitions per job : {max_batches_per_job}")
                    logger.info(f"   Rows per SF Job    : {batch_size * max_batches_per_job:,}")
                    if method == "upsert":
                        logger.info(f"   Ext ID Field       : {ext_id_field}")
                    logger.info(f"{'='*60}")

                    run_id          = str(uuid.uuid4())
                    total_fetched   = 0
                    success_count   = 0
                    failed_count    = 0
                    batch_num       = 0   # RDS partition counter
                    job_num         = 0   # SF Job counter
                    job_buffer      = []  # accumulates partitions until job is full
                    job_batch_count = 0   # partitions accumulated in current job

                    # ── Stream RDS rows — server-side cursor ──────────────────
                    with engine.connect() as read_conn:
                        result = read_conn.execute(
                            sa.text(source_query).execution_options(yield_per=batch_size)
                        )

                        for batch in result.partitions(batch_size):
                            batch_num += 1
                            records    = [dict(row._mapping) for row in batch]
                            job_buffer.extend(records)
                            job_batch_count += 1

                            # Free raw batch immediately — clean dicts are in job_buffer
                            del records, batch
                            gc.collect()

                            # ── Flush job when buffer reaches max_batches_per_job
                            if job_batch_count >= max_batches_per_job:
                                job_num += 1

                                row_results = push_batch_to_sf(
                                    sf=sf, sf_object=sf_object, method=method,
                                    records=job_buffer, id_column=id_column,
                                    ext_id_field=ext_id_field, batch_size=batch_size,
                                )

                                batch_success  = sum(1 for r in row_results if r["status"] == "SUCCESS")
                                batch_failed   = len(row_results) - batch_success
                                success_count += batch_success
                                failed_count  += batch_failed
                                total_fetched += len(job_buffer)

                                _write_row_batch(
                                    log_cur, schema, row_table,
                                    run_id, sf_object, method,
                                    job_num, row_results
                                )

                                logger.info(
                                    f"   Job {job_num} "
                                    f"(partitions {batch_num - job_batch_count + 1}→{batch_num}): "
                                    f"{len(job_buffer):,} records | "
                                    f"{batch_success:,} success | "
                                    f"{batch_failed:,} failed | "
                                    f"Total: {total_fetched:,} / {total_size:,}"
                                )

                                del job_buffer, row_results
                                gc.collect()
                                job_buffer      = []
                                job_batch_count = 0

                        # ── Flush remainder after loop ends ───────────────────
                        # Handles the final partial job (e.g. last 50 rows of 1,050)
                        # This is the CORRECT place — after the for loop is exhausted
                        if job_buffer:
                            job_num += 1

                            row_results = push_batch_to_sf(
                                sf=sf, sf_object=sf_object, method=method,
                                records=job_buffer, id_column=id_column,
                                ext_id_field=ext_id_field, batch_size=batch_size,
                            )

                            batch_success  = sum(1 for r in row_results if r["status"] == "SUCCESS")
                            batch_failed   = len(row_results) - batch_success
                            success_count += batch_success
                            failed_count  += batch_failed
                            total_fetched += len(job_buffer)

                            _write_row_batch(
                                log_cur, schema, row_table,
                                run_id, sf_object, method,
                                job_num, row_results
                            )

                            logger.info(
                                f"   Job {job_num} (final remainder "
                                f"partitions {batch_num - job_batch_count + 1}→{batch_num}): "
                                f"{len(job_buffer):,} records | "
                                f"{batch_success:,} success | "
                                f"{batch_failed:,} failed | "
                                f"Total: {total_fetched:,} / {total_size:,}"
                            )

                            del job_buffer, row_results
                            gc.collect()

                    # ── Write summary after all jobs complete ─────────────────
                    completed_at = datetime.now()
                    log_summary(
                        conn_str, schema, run_id,
                        target_table, sf_object, method, ext_id_field,
                        total_fetched, success_count, failed_count,
                        started_at, completed_at
                    )

                    logger.info(f"{'='*60}")
                    logger.info(f"[{job_key}] completed!")
                    logger.info(f"   SF Object    : {sf_object}")
                    logger.info(f"   Total Jobs   : {job_num:,}")
                    logger.info(f"   Success      : {success_count:,}")
                    logger.info(f"   Failed       : {failed_count:,}")
                    logger.info(f"   Duration     : {round((completed_at - started_at).total_seconds(), 2)}s")
                    logger.info(f"{'='*60}\n")
                    successful.append(job_key)
                
                else:
                    msg = "Object not selected in objects_to_load argument"
                    logger.info(f"Skipping '{job_key}': {msg}")
                    skipped.append(job_key)

            except ValueError as ve:
                completed_at = datetime.now()
                logger.error(f"Config error for '{job_key}': {ve} — Skipping.")
                failed.append({"job": job_key, "error": str(ve)})
                log_summary(conn_str, schema, str(uuid.uuid4()),
                            target_table, sf_object, method, ext_id_field,
                            0, 0, 0, started_at, completed_at, error_summary=str(ve))
                continue

            except Exception as e:
                completed_at = datetime.now()
                logger.error(f"Failed '{job_key}': {type(e).__name__}: {e} — Skipping.")
                failed.append({"job": job_key, "error": str(e)})
                log_summary(conn_str, schema, str(uuid.uuid4()),
                            target_table, sf_object, method, ext_id_field,
                            0, 0, 0, started_at, completed_at, error_summary=str(e))
                continue

            finally:
                # Always close persistent log connection
                if log_pg_conn:
                    try:
                        log_pg_conn.close()
                    except Exception:
                        pass
                gc.collect()

        # ── Final Summary ─────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info(f"RDS → SF Pipeline Summary — {datetime.now()}")
        logger.info(f"   Successful : {len(successful)} → {successful}")
        logger.info(f"   Skipped    : {len(skipped)}   → {skipped}")
        logger.info(f"   Failed     : {len(failed)}")
        for f in failed:
            logger.error(f"      - {f['job']}: {f['error']}")
        logger.info("=" * 60)

    return "RDS → SF Pipeline Run Completed"