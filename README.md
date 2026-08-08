# salesforce-postgres-sync

This repository provides a robust framework for two-way data synchronization between Salesforce and a PostgreSQL database. It uses Postgres as a powerful staging layer, enabling ETL (Extract, Transform, Load) workflows where data is extracted from Salesforce, transformed within Postgres, and then loaded back into Salesforce.

The framework is composed of three distinct, configurable pipelines:
1.  **Salesforce to Postgres**: Extracts data from Salesforce objects using SOQL and loads it into a raw schema in Postgres.
2.  **File to Postgres**: Loads data from local CSV or Excel files into Postgres, preserving data integrity by treating all columns as text.
3.  **Postgres to Salesforce**: Pushes data from staging tables in Postgres to Salesforce objects, supporting `insert`, `update`, and `upsert` operations via the Salesforce Bulk API.

## Features

*   **Two-Way Synchronization**: Full support for pulling data from and pushing data to Salesforce.
*   **Configuration Driven**: All pipeline operations are controlled through a central `configurations.yaml` file, allowing for easy management of objects, queries, and settings without code changes.
*   **Efficient Data Loading**: Utilizes the `dlt` (data load tool) library for efficient and reliable data extraction from Salesforce and files into Postgres. `dlt` is capable to work with large volume of data (millions of records) as it uses disk & batch instead of loading in memory everything.
*   **Large Volume Support**: The Postgres-to-Salesforce pipeline is optimized for large datasets, using server-side cursors for low memory usage, and intelligent batching to work with the Salesforce Bulk API limits.
*   **Detailed Auditing**: The Postgres-to-Salesforce pipeline creates detailed summary and row-level log tables in the database, tracking the status of every record pushed to Salesforce.
*   **Data Integrity**:
    *   File loading pipelines read all data as text to prevent automatic type casting and ensure source values are perfectly preserved.
    *   Nested JSON data from Salesforce is automatically flattened into a tabular format with underscore-separated column names.

## How It Works

### 1. Salesforce to Postgres
This pipeline reads configurations from the `source_objects` section of the YAML file.
- It connects to Salesforce using the provided credentials.
- For each active object, it executes the specified SOQL query.
- It uses the `dlt` library to stream the results, automatically handling pagination.
- Nested data (e.g., `RecordType.Name`) is flattened into `recordtype_name`.
- The data is loaded into the target Postgres table and schema, with support for `replace`, `append`, or `merge` operations.

### 2. File to Postgres
This pipeline reads configurations from the `file_to_rds` section of the YAML file.
- It identifies the file type (CSV, XLSX, or XLS) based on the extension.
- It uses custom readers (`pandas` for CSV, `openpyxl` for Excel) to read all data as string values, preventing data corruption from type inference.
- It uses the `dlt` library to load the data into the specified Postgres table and schema.
- It supports `replace`, `append`, or `merge` write dispositions.

### 3. Postgres to Salesforce
This pipeline reads configurations from the `target_objects` section of the YAML file.
- It connects to a Postgres database and executes the specified SQL query using a server-side cursor to handle large result sets without high memory consumption.
- It streams the data in batches and groups them into jobs for the Salesforce Bulk API.
- It prepares the payload based on the operation (`insert`, `update`, `upsert`) and pushes it to the specified Salesforce object.
- After each job completes, it writes detailed row-level results (success/failure, new ID, error messages) and a final summary record to dedicated log tables in Postgres.

## Setup
1.  **Install Python:**
    Python 3.9.10 

2.  **Clone the repository:**
    ```bash
    git clone https://github.com/giteshdkolte/salesforce-postgres-sync.git
    cd salesforce-postgres-sync
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Create an environment file:**
    Create a `.env` file in the root directory of the project and add your credentials. This file stores sensitive information and should not be committed to version control.

    ```env
    # Salesforce Connected App Credentials
    client_id=<YOUR_SALESFORCE_CLIENT_ID>
    client_secret=<YOUR_SALESFORCE_CLIENT_SECRET>
    url=<YOUR_SALESFORCE_INSTANCE_URL>
    sf_domain=<SALESFORCE_DOMAIN>

    # PostgreSQL Connection String
    pg_connection_string=postgresql://<user>:<password>@<host>:<port>

    # DLT working directory
    dlt_pipeline_disk_location=<DLT_MANAGED_PIPELINES_DIRECTORY>
    ```

    ## Example
    ```env
    url = "https://mycompany.my.salesforce.com"
    client_id = "********"
    client_secret = "********"
    sf_domain = "mycompany.my"
    pg_connection_string = "postgresql://postgres:postgres@localhost:5432"
    # DLT uses disk and not memory, millions of records can be processed with ease using DLT
    # This location is used to store the metadata & data of DLT
    dlt_pipeline_disk_location = "D:/dlt_data"    # In Windows
    # dlt_pipeline_disk_location = "/mnt/migration/dlt_data"    # In Linux
    ```
  ### Note: 
  See your file system disk space using command `df -h` and choose the mount based on the disk space and volume of data.

## Configuration (`config/configurations.yaml`)

All pipeline behavior is defined in `config/configurations.yaml`. The file is structured into sections for each pipeline type.

### General Settings
A `default_settings` anchor defines common properties that can be inherited by any object configuration.

```yaml
default_settings: &defaults
  rds_db_name: salesforce_db
  rds_schema_name: salesforce                     # For SF->RDS and File->RDS
  rds_staging_schema_name: salesforce_staging     # For RDS->SF
  src_operation: replace                          # replace, append, merge
  batch_size: 1000
```

### Salesforce to Postgres (`source_objects`)
Define each Salesforce object you want to extract.

- `active_status`: `Y` to enable, `N` to disable.
- `soql`: The SOQL query to extract the data.
- `target_table`: The name of the table to create in Postgres.
- `src_operation`: `replace` (full reload), `append` (add new rows), or `merge` (upsert on a primary key).

```yaml
source_objects:
  Case:
    <<: *defaults
    active_status: Y
    soql: |
      SELECT Id, CaseNumber, ContactId, AccountId, Status, Subject
      FROM CASE
    target_table: case

  Opportunity:
    <<: *defaults
    active_status: Y
    src_operation: append
    soql: |
      SELECT Id, Name, StageName, Amount, CloseDate
      FROM Opportunity
    target_table: opportunity
```

### File to Postgres (`file_to_rds`)
Define each file you want to load. The key (e.g., `contact_load_excel`) becomes the target table name.

- `active_status`: `Y` to enable, `N` to disable.
- `file_path`: Absolute or relative path to the source file.
- `sheet_name` (Excel only): The sheet to read (can be name or 0-based index).
- `delimiter` (CSV only): The column delimiter.

```yaml
file_to_rds:
  contact_load_excel:
    <<: *defaults
    active_status: Y
    file_path: "data/contacts.xlsx"
    sheet_name: 0

  account_load_csv:
    <<: *defaults
    active_status: Y
    file_path: "data/accounts.csv"
    delimiter: ","
```

### Postgres to Salesforce (`target_objects`)
Define each object you want to push data to.

- `active_status`: `Y` to enable, `N` to disable.
- `api_name`: The API name of the target Salesforce object (e.g., `Case`).
- `sf_operation`: `insert`, `update`, or `upsert`.
- `sql`: The SQL query to select data from your Postgres staging table. You can use `{{SCHEMA}}` as a placeholder for the `rds_staging_schema_name`.
- `old_id_column`: The primary key from your source Postgres table, used for logging and for matching records during an `update`.
- `external_id_field` (for `upsert` only): The API name of the External ID field in Salesforce.
- `target_table`: The name of the source Postgres table. This is used to create the corresponding row-level log table.

```yaml
target_objects:
  Case:
    <<: *defaults
    active_status: Y
    api_name: Case
    sf_operation: insert
    sql: |
      SELECT *
      FROM {{SCHEMA}}.salesforce_case_transformed
    target_table: salesforce_case_transformed

  Opportunity:
    <<: *defaults
    active_status: Y
    api_name: Opportunity
    sf_operation: update
    old_id_column: "id"
    sql: |
      SELECT id, amount
      FROM salesforce_staging.salesforce_opportunity_transformed
    target_table: salesforce_opportunity_transformed
```

## Usage

The main entry point is `main.py`. You must choose one pipeline to run at a time.

### Run Salesforce to Postgres Pipeline

-   **For all active objects in the config:**
    ```bash
    python main.py --salesforce_to_rds
    ```
-   **For specific objects:**
    ```bash
    python main.py -sf_2_rds -o Case Opportunity
    ```

### Run File to Postgres Pipeline

-   **For all active files in the config:**
    ```bash
    python main.py --file_to_rds
    ```

### Run Postgres to Salesforce Pipeline

-   **For all active objects in the config:**
    ```bash
    python main.py --rds_to_salesforce
    ```
-   **For a specific object:**
    ```bash
    python main.py -rds_2_sf -o Case
    ```

## Logging

### Pipeline Logs
A general log file is created for each run in the `logs/` directory (e.g., `pipeline_20231027_103000.log`), capturing output from all pipeline types.

### Database Audit Logs (RDS to Salesforce)
For the `rds_to_salesforce` pipeline, two types of log tables are automatically created in the `rds_staging_schema_name` schema within your Postgres database for detailed auditing:

1.  **Summary Table (`pipeline_sf_load_summary`)**: A single table that records one entry per pipeline run, summarizing the results.
    -   `run_id`: A unique ID for the run.
    -   `total_rows`, `success_count`, `failed_count`.
    -   `started_at`, `completed_at`, `duration_seconds`.
    -   `status` (`SUCCESS`, `FAILED`, `PARTIAL`).

2.  **Row-Level Detail Table (`<target_table>_level_row_information`)**: A dedicated table is created for each source table (e.g., `salesforce_case_transformed_level_row_information`). It records the outcome for every single row that was processed.
    -   `run_id`: Links back to the summary table.
    -   `old_id`: The original primary key from the Postgres source table.
    -   `new_id`: The new Salesforce ID for successful inserts.
    -   `status`: `SUCCESS` or `FAILED`.
    -   `error_message`: Detailed error message from the Salesforce API for failed rows.

## Troubleshooting

### What if a table in Postgres gets accidentally deleted? (Salesforce → Postgres and File → Postgres)

If a target table is dropped (accidentally or intentionally) and you need `dlt` to recreate it cleanly, both the Postgres-side metadata and the local pipeline state must be cleared together — otherwise `dlt` will think the table still exists and skip recreating it.

**On Postgres:**

Drop the `dlt` internal metadata tables from the relevant schema:

```sql
DROP TABLE salesforce."_dlt_loads";
DROP TABLE salesforce."_dlt_version";
DROP TABLE salesforce."_dlt_pipeline_state";
DROP TABLE {target_table_name};  -- Only if you intentionally want the schema to change/update
```

**On the terminal (Linux/macOS — adjust paths for Windows):**

Clear the local pipeline state so `dlt` doesn't reference stale metadata:

```bash
export DLT_DATA_DIR=/mnt/migration/dlt_data
dlt pipeline sf_to_rds drop-pending-packages
rm -rf /mnt/migration/dlt_data/pipelines/sf_to_rds/
```

**Notes:**
- Pipeline names vary depending on the source (Salesforce or file load). Anyway there are two pipelines in this project as of now which use DLT, Salesforce to RDS - sf_to_rds & File to RDS - file_to_rds. `sf_to_rds` is used here as an example — check your actual pipeline name with: 
  ```bash
  ls /mnt/migration/dlt_data/pipelines/
  ```
- Only drop `{target_table_name}` if you *intend* for its schema to change or be rebuilt. **Do not drop it accidentally** — there's no automatic backup.
- Clearing `dlt` metadata and pending packages does not affect other tables **unless** they use `append` mode. Since this project's pipelines are configured for `replace` mode, this cleanup is isolated and safe.
