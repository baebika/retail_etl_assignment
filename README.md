# Assignment 2.3 – Retail Data Warehouse ETL

## Project overview

This project implements a retail data warehouse ETL pipeline using **Apache Airflow**, **Docker Compose**, and **Snowflake**.

The Airflow DAG (`retail_etl`) orchestrates the daily pipeline:

1. Create a batch and capture run details.
2. Ingest source files into the Bronze layer.
3. Transform and standardize data in the Silver layer.
4. Validate Silver data quality.
5. Load dimensions, facts, and aggregates into the Gold layer.
6. Run reconciliation checks and store their results in the OPS schema.
7. Mark the batch as successful. Failed runs are handled by the DAG failure callback.

The Snowflake database used by the project is `RETAIL_DW`, with the `BRONZE`, `SILVER`, `GOLD`, `AGG`, and `OPS` schemas.

## Technologies

- Apache Airflow 3.3.2
- Docker Compose
- Snowflake
- Python
- Snowflake Airflow provider

## Project structure

```text
Assignment2.3 - project/
│
├── Config/                 # Airflow or project configuration files
├── Dags/                   # Airflow DAGs and Python pipeline modules
├── Data/                   # Input/source data files used by the pipeline
├── Evidence/               # Assignment screenshots and validation evidence
├── Logs/                   # Airflow task execution logs (generated at runtime)
├── Sql/                    # SQL scripts for warehouse objects and transformations
├── Tests/                  # Project tests and validation scripts
├── Venv/                   # Local Python virtual environment
│
├── .env                    # Local environment variables and credentials
├── docker-compose.yaml     # Docker Compose services
├── Dockerfile              # Custom Airflow image definition
├── README.md               # Project documentation
└── requirements.txt         # Python dependencies
```

`Venv/` and `Logs/` are local/runtime folders. They do not need to be committed to GitHub. The `.env` file contains local configuration and credentials. 

## Prerequisites

- Docker Desktop installed and running with the Linux container engine.
- Docker Compose available (`docker compose`).
- Access to the Snowflake account and the `RETAIL_DW` database.
- Project source files and SQL scripts in the expected locations.
- Required Python dependencies installed in the Airflow image.

## Configuration

1. Create or update the local `.env` file in the project root.
2. Set the environment variables referenced by `docker-compose.yaml`, including the required Airflow settings and `FERNET_KEY`.
3. Configure the Airflow connection with connection ID **`snowflake_default`**. Supply the correct Snowflake account, username, password or supported authentication method, warehouse, database, schema, and role.
4. Check that the Dockerfile installs the dependencies required by the DAG and that `requirements.txt` contains the project's Python dependencies.
5. Do not commit `.env`, passwords, private keys, or other credentials to source control.

The supplied Docker Compose configuration is intended for local development and assignment work, not production deployment.

## How to run

Run the following commands in PowerShell from the project root (the folder containing `docker-compose.yaml`).

### 1. Start Docker Desktop

Wait until the Docker engine is running.

### 2. Initialize Airflow

```powershell
docker compose up airflow-init
```

### 3. Start the services

```powershell
docker compose up -d
```

### 4. Check container status

```powershell
docker compose ps
```

Confirm that the Airflow services, PostgreSQL, and Redis are running and healthy.

### 5. Open the Airflow UI

Open [http://localhost:8080](http://localhost:8080).

Sign in using the Airflow administrator credentials configured in the Compose environment.

### 6. Check the DAG

```powershell
docker compose exec airflow-scheduler airflow dags list
```

Check for DAG import errors:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

The DAG ID is `retail_etl`. Trigger it from the Airflow UI after confirming that the Snowflake connection and required source files are available.

## How to backfill

A backfill creates DAG runs for a historical date range. For example, to backfill **10 November 2026 through 14 November 2026**, run this command from the project root:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id retail_etl --from-date 2026-11-10 --to-date 2026-11-14
```

Change the dates to the business-date range you need. The specified dates are inclusive.

After creating a backfill:

1. Monitor the DAG runs and task logs in the Airflow UI.
2. Inspect failed tasks and resolve the underlying issue before retrying.
3. Verify the reconciliation results in Snowflake. Creating a backfill does not guarantee that every run succeeded.

## Reconciliation and evidence

The latest reconciliation result for each check and business date can be reviewed using `RETAIL_DW.OPS.VW_RECON_LATEST`:

```sql
SELECT
    BUSINESS_DATE,
    BATCH_ID,
    CHECK_CODE,
    EXPECTED_VALUE,
    ACTUAL_VALUE,
    STATUS,
    CHECKED_TS
FROM RETAIL_DW.OPS.VW_RECON_LATEST
WHERE BUSINESS_DATE BETWEEN '2026-11-10' AND '2026-11-14'
ORDER BY BUSINESS_DATE, CHECK_CODE;
```

To see a summary of passed and failed checks by date:

```sql
SELECT
    BUSINESS_DATE,
    COUNT(*) AS TOTAL_CHECKS,
    COUNT_IF(STATUS = 'PASSED') AS PASSED_CHECKS,
    COUNT_IF(STATUS = 'FAILED') AS FAILED_CHECKS
FROM RETAIL_DW.OPS.VW_RECON_LATEST
WHERE BUSINESS_DATE BETWEEN '2026-11-10' AND '2026-11-14'
GROUP BY BUSINESS_DATE
ORDER BY BUSINESS_DATE;
```

Save assignment screenshots and other required validation outputs in the `Evidence/` folder. For final evidence, capture the detailed reconciliation results and the summary after the final backfill. A successful validation should show zero failed checks for each processed date.

## Known limitations and considerations

- **Local development setup:** The Docker Compose environment is designed for local development and is not production-hardened.
- **Snowflake dependency:** The pipeline requires a working Snowflake account, appropriate permissions, a warehouse, the required database objects, and the Airflow connection `snowflake_default`.
- **Source-file dependency:** Each business date depends on the expected source files being available and matching the naming and schema assumptions in the ingestion code.
- **Historical backfills:** Reconciliation for a historical date must compare data valid for that business date. Comparing a historical snapshot against only today's current inventory positions can produce misleading failures.
- **SCD Type 2 date validity:** Dimension history must have valid effective date ranges. An `EFF_TO_DT` earlier than `EFF_FROM_DT` is invalid and can cause the location-date reconciliation check to fail.
- **Source-data gaps:** Missing or inconsistent source records can affect downstream facts and reconciliation. Do not invent missing business data to force a check to pass; document source issues and follow the project's exception or quarantine process.
- **Optional or unimplemented feeds:** `DIM_CUSTOMER` and `FCT_PRICE` are not populated by the current implementation where the required source feeds are unavailable or outside the implemented scope.
- **Reconciliation audit history:** `OPS_RECON_RESULT` can contain multiple attempts for the same date and check. Use `VW_RECON_LATEST` to review the latest result per business date and check, while retaining the underlying audit rows for traceability.

## Troubleshooting

- **Docker engine connection error:** Start Docker Desktop and confirm that it is using the Linux container engine.
- **DAG not appearing:** Run `airflow dags list-import-errors` and inspect the scheduler or DAG processor logs.
- **Snowflake connection failure:** Verify the `snowflake_default` connection, credentials, network access, role, warehouse, and database permissions.
- **Reconciliation failure:** Inspect failed rows in `RETAIL_DW.OPS.OPS_RECON_RESULT`. Investigate the source data, Silver transformation, Gold data, or reconciliation logic. Fix the root cause before rerunning the affected dates.
- **Missing local log directory:** Recreate the `Logs/` directory if needed. Airflow writes task logs according to the log volume configuration in `docker-compose.yaml`.

## Stopping the services

```powershell
docker compose down
```

This stops and removes the Compose containers and network. It does not remove the named PostgreSQL volume. Avoid `docker compose down -v` unless you intentionally want to delete the persisted Airflow metadata database.

---
