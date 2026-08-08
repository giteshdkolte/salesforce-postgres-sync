# salesforce_source.py
# Salesforce extraction logic

import os
from dotenv import load_dotenv

load_dotenv()
os.environ["DLT_DATA_DIR"] = os.getenv("dlt_pipeline_disk_location")

import dlt
from simple_salesforce import Salesforce
from typing import Iterator, List
from utils.salesforce_flatten import process_records
import re
import logging

# Define a specific logger name instead of the root logger
logger = logging.getLogger("data_migration_pipeline") 

def get_sf_connection(
    instance_url: str,
    consumer_key: str,
    consumer_secret: str,
    domain: str
) -> Salesforce:
    """
    Create Salesforce connection
    """
    return Salesforce(
        instance_url = instance_url,
        consumer_key = consumer_key,
        consumer_secret = consumer_secret,
        domain = domain
    )

def extract_object(
    sf: Salesforce,
    soql: str,
    object_name: str
) -> Iterator[dict]:
    """
    Extract records from Salesforce
    in chunks via pagination
    yields one record at a time
    dlt handles the rest
    """
    logger.info(f"Extracting {object_name}...")
    
    total_fetched = 0
    batch_num = 0
    
    # First batch
    result = sf.query(soql)
    total_size   = result.get('totalSize', '?')
    logger.info(f"Total records reported by SF : {total_size:,}\n")

    batch_num += 1
    
    while True:
        records = result['records']
        records_cleaned = process_records(records)

        for record in records_cleaned:
            total_fetched += 1
            yield record # dlt handles batching
        
        logger.info(
            f" Batch {batch_num}: "
            f"{len(records)} records | "
            f"Total: {total_fetched} / {total_size:,}"
        )
        
        # All batches done
        if result['done']:
            break
        
        # Fetch next batch
        result = sf.query_more(
            result['nextRecordsUrl'],
            identifier_is_url=True
        )
        batch_num += 1
    
    logger.info(
        f" {object_name} complete: "
        f"{total_fetched} records"
    )


def get_columns_from_soql(soql: str) -> dict:
    """
    Extract column names from SOQL query
    and define all as text nullable
    so NULL columns still get created in DB
    """
    # Extract fields between SELECT and FROM
    match = re.search(r'SELECT\s+(.+?)\s+FROM\s', soql, re.IGNORECASE | re.DOTALL)
    if not match:
        return {}
    
    fields_str = match.group(1)
    
    # Split by comma and clean up
    fields = [f.strip().lower().replace('.', '_') for f in fields_str.split(',')]
    
    # Build dlt columns dict
    columns = {
        field: {
            "data_type" : "text",
            "nullable"  : True
        }
        for field in fields
        if field  # skip empty
    }
    return columns

@dlt.source
def salesforce_source(
    secrets: dict,
    source_config: dict = None
):
    """
    dlt source for Salesforce
    Define all objects to extract here
    """

    instance_url = secrets.get(f"url")
    consumer_key = secrets.get(f"client_id")
    consumer_secret = secrets.get(f"client_secret")
    domain = secrets.get(f"sf_domain")

    sf = get_sf_connection(
        instance_url,
        consumer_key,
        consumer_secret,
        domain
    )

    # Define the source level configurations
    table_name = source_config.get("target_table")
    prefix = source_config.get("rds_table_prefix", None)
    if prefix:
        table_name = f"{prefix}_{table_name}"

    soql = source_config["soql"]
    src_operation = source_config["src_operation"].lower()
    
    # Find the operation type 
    if src_operation == 'replace':
        # Replace — full reload every run
        operation = {
            "disposition": "replace",
            "strategy": "truncate-and-insert"
        }
    elif src_operation == 'append':
        # Append — add new records each run
        operation = "append"
    elif src_operation == 'merge':
        # Merge — upsert based on primary key
        operation = {
            "disposition": "merge",
            "strategy": "upsert",
            "primary_key": source_config.get("src_merge_key")
        }
    else:
        raise ValueError("The operation type can be replace, append or merge!")

    @dlt.resource(
        name                =   table_name, 
        write_disposition   =   operation,
        columns             =   get_columns_from_soql(soql)
    )
    def resource_fn():
        yield from extract_object(sf, soql, table_name)
    
    return resource_fn