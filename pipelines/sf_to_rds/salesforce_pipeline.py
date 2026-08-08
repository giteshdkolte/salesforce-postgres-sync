# sf_pipeline.py
# Main pipeline runner

import os
from dotenv import load_dotenv

load_dotenv()
os.environ["DLT_DATA_DIR"] = os.getenv("dlt_pipeline_disk_location")

import dlt

# ── Override naming convention ────────────────────────────
dlt.config["schema.naming"] = "direct"
dlt.config["schema.allow_identifier_change_on_table_with_data"] = True

from dlt.destinations import postgres
from pipelines.sf_to_rds.salesforce_source import salesforce_source
import logging
from datetime import datetime
import yaml
from utils.dot_env_secrets import load_env_as_dict
import gc

# Define a specific logger name instead of the root logger
logger = logging.getLogger("data_migration_pipeline")

# Pipeline Setup
def run_pipeline(objects_to_load: list = None) -> str:
    """
    Run Salesforce to RDS pipeline
    
    objects_to_load: list of specific
    objects to load, None = load all
    """
    
    with open(f"./config/configurations.yaml") as stream:
        # Configurations
        config = yaml.safe_load(stream)
        # Get secret keys & values
        secrets = load_env_as_dict(".env")

        # ── Define your objects and SOQL from config ─────────────────────
        src_objs = config.get("source_objects")

        # Ignore the objects which are not needed
        if objects_to_load:
            objects_to_load = [obj.lower() for obj in objects_to_load]
        else:
            objects_to_load = [obj.lower() for obj in list(src_objs.keys())]

        # ── success and failures across all objects ─────────────────
        successful_objects = []
        failed_objects     = []
        skipped_objects    = []

        # Iterate to the objects as a individual pipeline
        for k, v in src_objs.items():
            try:                
                # Run the pipeline for active status Y
                if v["active_status"].upper() == 'Y':
                    if k.lower() in objects_to_load:
                        rds_schema_name = v["rds_schema_name"]
                        db_name = v["rds_db_name"]
                        rds_pg_conn_str = f"{secrets.get(f'pg_connection_string')}/{db_name}"

                        # ── Create pipeline ───────────────────────────────────
                        pipeline = dlt.pipeline(
                            pipeline_name='sf_to_rds',
                            destination=postgres(credentials=rds_pg_conn_str),
                            dataset_name=rds_schema_name, # schema name in RDS
                            dev_mode=False
                        )

                        # ── Get source ────────────────────────────────────────
                        source = salesforce_source(
                            secrets=secrets,
                            source_config=v
                        )

                        # ── Filter specific objects if needed ─────────────────
                        # if objects_to_load:
                        #     source = source.with_resources(*objects_to_load)
                        
                        # ── Run pipeline ──────────────────────────────────────
                        logger.info(f"{'='*60}")
                        logger.info(f"Starting SF → RDS Pipeline")
                        logger.info(f"Time: {datetime.now()}")
                        logger.info(f"{'='*60}")
                        
                        load_info = pipeline.run(source)
                        
                        # ── Check for dlt silent failures ─────────────────────────
                        # dlt does not always raise exceptions — it sometimes stores
                        # errors inside load_info. So we check both.
                        if load_info.has_failed_jobs:
                            failed_jobs_detail = [str(job) for job in load_info.failed_jobs]
                            raise RuntimeError(f"dlt reported failed jobs: {failed_jobs_detail}")

                        # ── Print results ─────────────────────────────────────
                        logger.info(f"{'='*60}")
                        logger.info("PIPELINE RESULTS")
                        logger.info(f"{'='*60}")
                        logger.info(load_info)
                        successful_objects.append(k)
                    else:
                        msg = "Object not selected in objects_to_load argument"
                        logger.info(f"Skipping '{k}': {msg}")
                        skipped_objects.append(k)

                # Skip the objects having active set as No
                elif v["active_status"].upper() == 'N':
                    msg = "active_status=N — set to Y in configurations to enable"
                    logger.info(f"Skipping '{k}': {msg}")
                    skipped_objects.append(k)
                # Skip the objects having incorrect active status
                else:
                    msg = f"Invalid active_status='{v.get('active_status')}' — expected Y or N"
                    logger.warning(f"Skipping '{k}': {msg}")
            
            except KeyError as ke:
                # Missing config key (e.g. rds_schema_name not defined in YAML)
                logger.error(
                    f"Config key missing for object '{k}': {ke} — Skipping."
                )
                failed_objects.append({"object": k, "error": f"Missing config key: {ke}"})
                continue

            except RuntimeError as re:
                # dlt silent job failures caught above
                logger.error(
                    f"dlt job failure for object '{k}': {re} — Skipping."
                )
                failed_objects.append({"object": k, "error": str(re)})
                continue

            except Exception as e:
                # Catch all other errors (connection issues, SF API errors, etc.)
                logger.error(
                    f"Failed to load object '{k}': {type(e).__name__}: {e} — Skipping."
                )
                failed_objects.append({"object": k, "error": str(e)})
                continue

            finally:
                # Always free RAM after each object, whether it passed or failed
                gc.collect()

        logging.info('\n')
        # ── Final Summary ─────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info(f"Pipeline Summary — {datetime.now()}")
        logger.info(f"   Successful : {len(successful_objects)} object(s) → {successful_objects}")
        logger.info(f"   Skipped   : {len(skipped_objects)} object(s)  → {skipped_objects}")
        logger.info(f"   Failed     : {len(failed_objects)} object(s)")
        
        for failure in failed_objects:
            logger.error(f"      - {failure['object']}: {failure['error']}")
        
        logger.info("=" * 60)
        logging.info('\n')
    
    return "Pipeline Run Completed"