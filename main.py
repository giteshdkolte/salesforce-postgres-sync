from pipelines.sf_to_rds.salesforce_pipeline import run_pipeline
from pipelines.rds_to_sf.rds_to_sf_pipeline import run_rds_to_salesforce
from pipelines.file_to_rds.excel_csv_to_rds_pipeline import run_excel_csv_to_rds
from datetime import datetime
import logging
import argparse
import os 

# Apply logger
#----------------------------------------------------------------------------
os.makedirs('./logs/', exist_ok=True)
log_format = '%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(
    filename=f'./logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format=log_format
)

logger = logging.getLogger("data_migration_pipeline")
# Create the console handler
console_handler = logging.StreamHandler()
# Create and set the formatter for the console handler
formatter = logging.Formatter(log_format)
console_handler.setFormatter(formatter)
# Add the handler to the logger
logger.addHandler(console_handler)

#----------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='ISM Framework')

# Argument: Objects
parser.add_argument('--objects', '-o', type=str, nargs='*',
                    help='Objects eligible to extract from Salesforce to RDS or RDS to Salesforce')

# Mutually exclusive group: exactly one mechanism must be chosen
mechanism_group = parser.add_mutually_exclusive_group(required=True)

# Argument: Salesforce to RDS
mechanism_group.add_argument('--salesforce_to_rds', '-sf_2_rds', action='store_true',
                    help='Mechanism to extract data from Salesforce object to RDS')
# Argument: RDS to Salesforce
mechanism_group.add_argument('--rds_to_salesforce', '-rds_2_sf', action='store_true',
                    help='Mechanism to extract data from RDS staging table to Salesforce object')
# Argument: File to RDS
mechanism_group.add_argument('--file_to_rds', '-file_2_rds', action='store_true',
                    help='Mechanism to extract data from file [CSV, XLSX or XLS] to RDS')

args = parser.parse_args()

objects = args.objects
sf_to_rds_flg = args.salesforce_to_rds
file_to_sf_flg = args.file_to_rds
rds_to_sf_flg = args.rds_to_salesforce

# Determine whether to load all objects or specific ones
load_all_objects = (objects == ["all"] or not objects)

# Salesforce -> RDS 
if sf_to_rds_flg:
    if load_all_objects:
        logger.info(run_pipeline())
    else:
        logger.info(run_pipeline(objects_to_load=objects))

# File -> RDS 
elif file_to_sf_flg:
    logger.info(run_excel_csv_to_rds())

# RDS -> Salesforce
elif rds_to_sf_flg:
    if load_all_objects:
        logger.info(run_rds_to_salesforce())
    else:
        logger.info(run_rds_to_salesforce(objects_to_load=objects))

