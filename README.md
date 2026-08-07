# salesforce-postgres-sync
Two-way sync between Salesforce and Postgres — pulls data in with DLT, pushes it back with batched/partitioned psycopg2. Uses Postgres as a staging layer for ETL (Extract from Salesforce, Transform in Postgres, Load into Salesforce).
