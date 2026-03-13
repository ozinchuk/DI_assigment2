# DI_assigment2

# Telephony Analytics ETL Pipeline

## Project Overview
This project implements an hourly Apache Airflow ETL pipeline designed to provide near-real-time visibility into support call quality. The DAG extracts raw call records from a MySQL database, enriches them with simulated telephony metadata (parsed from JSON files), and loads the final, clean dataset into a DuckDB analytical database for downstream reporting.

## Architecture & Technologies
* **Orchestration:** Apache Airflow (Dockerized via Astro CLI)
* **Source Database:** MySQL (Employee and Call records)
* **External API / Mock Data:** Local filesystem (JSON files representing telephony provider responses)
* **Target Data Warehouse:** DuckDB
* **Data Processing:** Python (Pandas)

## Pipeline Workflow (DAG Tasks)

The DAG `telephony_analytics_etl` is scheduled to run `@hourly` and consists of three core tasks:

### 1. `detect_new_calls`
* Connects to the MySQL database using Airflow Connections to prevent hardcoded credentials.
* Uses an Airflow Variable (`last_loaded_call_time`) as a watermark to implement incremental extraction.
* Queries only the calls that occurred after the last successful load.
* Passes the extracted raw call data downstream via XCom.

### 2. `load_telephony_details`
* Retrieves the new `call_id`s from XCom.
* Dynamically locates and reads the corresponding JSON files from the simulated telephony service.
* Performs schema validation to ensure required fields (`call_id`, `duration_sec`, `short_description`) are present.
* Implements basic data quality checks (e.g., rejecting files with negative call durations).

### 3. `transform_and_load_duckdb`
* Pulls data from the previous tasks and fetches employee dimension data from MySQL.
* Joins the raw calls, employee details, and telephony metadata using Pandas.
* Connects to the local DuckDB instance.
* Performs an **upsert** operation based on `call_id` to ensure absolute **idempotency**. Re-running the DAG will update existing records rather than creating duplicates.
* Safely updates the Airflow Variable (`last_loaded_call_time`) only after a successful database transaction.

## Key Engineering Features
* **Idempotency:** Implemented via DuckDB merge/upsert logic.
* **Incremental Loading:** Minimized database load by strictly fetching new rows based on a dynamic watermark.
* **Observability:** Custom logging implemented to track row counts, join sizes, and rejected JSON files.
* **Alerting:** An `on_failure_callback` is configured to log specific alerts if a task fails.
* **Backfill-Friendly:** Configured with `catchup=False` to prevent scheduler overload during deployment.
