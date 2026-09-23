# flight-aviation-pipeline

This project demonstrates a production-grade aviation data pipeline built on the **Databricks Lakehouse Platform** and managed via **Unity Catalog**. 

The pipeline ingests raw, semi-structured flight logs from an **Azure Data Lake Storage (ADLS Gen2)** landing zone through Managed Volumes. Employing a strict **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**, it implements an in-place file tracking mechanism for incremental ingestion and enforces a **Quarantine Pattern (`_bad_rec`)** to isolate corrupt schema records without breaking the pipeline. Validated records undergo SCD Type 1 upserts in the Silver layer and are aggregated into analytics-ready **Gold Delta Tables**, delivering high-performance operational metrics for flight delays and carrier performance. The entire workflow is fully orchestrated as an idempotent multi-task DAG using **Databricks Workflows**.

---

## Architecture & Data Flow

<img width="1111" height="637" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/563791ad-0e41-43c0-9f06-311c063a7c6f" />

Orchestrated via Databricks Workflows (`Flight job`):

<img width="1211" height="438" alt="Databricks Workflow DAG" src="https://github.com/user-attachments/assets/f5115530-f6ae-4e2b-9cac-569f8b95bd39" />

1. **Bronze (Raw Ingestion):**
   * Reads raw flight CSVs from Databricks Managed Volumes.
   * Instead of moving or archiving files, it compares source file paths against `_file_path` in the Bronze table to only ingest new files (in-place tracking).
   * Appends essential audit columns (`_load_timestamp`, `_file_path`).

2. **Silver (Cleaned & Conformed):**
   * Cleans data types, validates business constraints, and deduplicates records.
   * Invalid or corrupt records (e.g., missing business keys) are routed to a separate `_bad_rec` quarantine table with failure reasons logged, rather than failing the entire pipeline.
   * Merges valid records into the Silver Delta table using SCD Type 1 (`DeltaTable.merge`) to handle updates idempotently.

3. **Gold (Aggregations):**
   * Computes daily flight metrics, carrier delays, and cancellation rates for downstream analytics.
   * Uses partition overwrite to keep aggregated summaries continuously up to date.

---

## Setup & Execution Guide

### 1. Prerequisites
* Databricks Workspace enabled with **Unity Catalog**.
* Azure Data Lake Storage (ADLS Gen2) configured as the backing storage.
* Databricks Runtime 14.x+ (Spark 3.5+).

### 2. Unity Catalog Preparation
Run the following SQL commands in a Databricks SQL Editor or Notebook to initialize the environment:

```sql
-- 1. Create Catalog and Schema
CREATE CATALOG IF NOT EXISTS flight_catalog;
USE CATALOG flight_catalog;

CREATE SCHEMA IF NOT EXISTS flight_schema;
USE SCHEMA flight_schema;

-- 2. Create Managed Volume for raw ingestion landing
CREATE VOLUME IF NOT EXISTS flight_catalog.flight_schema.raw_flight_volume;
```

### 3. Data Ingestion Landing
Upload the raw flight tracking CSV files into the created Volume path:
`/Volumes/flight_catalog/flight_schema/raw_flight_volume/`

### 4. Git Folder Integration
1. In your Databricks Workspace, navigate to **Workspace** $\rightarrow$ **Repos** (or **Git Folders**).
2. Click **Add Repo** and clone this repository.
3. Keep the notebook directory relative structure intact (`%run ./Framework`).

### 5. Databricks Workflow Setup
1. Go to **Workflows** $\rightarrow$ **Jobs** $\rightarrow$ **Create Job**.
2. Configure **3 sequential tasks** pointing to the notebooks in your Git folder:
   * **Task 1 (`Flight_bronze`):** Points to `Bronze FW.py`
   * **Task 2 (`Flight_silver`):** Points to `Silver FW.py` (Depends on: `Flight_bronze`)
   * **Task 3 (`Flight_gold`):** Points to `Gold.py` (Depends on: `Flight_silver`)
3. Click **Run now** to trigger the complete pipeline.

---

## Project Structure

Everything is split into modular runner notebooks and a shared framework class:

```text
├── README.md
├── Framework.py       # Reusable Bronze & Silver ETL logic, logger, and Delta helpers
├── Config.py          # Metadata configurations and table schemas
├── setup.py           # Catalog / schema / volume initialization
├── Bronze FW.py       # Bronze layer ingestion runner
├── Silver FW.py       # Silver layer validation, quarantine, & merge runner
├── Gold.py            # Gold layer analytical aggregation runner
└── run.py             # Local entrypoint to trigger all layers sequentially
```
