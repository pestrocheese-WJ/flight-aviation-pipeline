# flight-aviation-pipeline
Flight data pipeline on Databricks using Delta Lake and Medallion Architecture (Bronze, Silver, Gold)
# Flight Data Pipeline (Databricks Medallion)

A batch pipeline built on Databricks to process flight operational data using a classic Medallion architecture (Bronze -> Silver -> Gold).

The main goal here was to build a clean, modular pipeline that handles incremental ingestion and data cleansing without overcomplicating things.

---

## Architecture & Data Flow

Orchestrated via Databricks Workflows (`Flight job`):

1. **Bronze (Raw Ingestion):**
   * Reads raw flight CSVs from Databricks Volumes.
   * Instead of moving or archiving files, it compares source file paths against `_file_path` in the Bronze table to only ingest new files (in-place tracking).
   * Appends basic audit columns (`_load_timestamp`, `_file_path`).

2. **Silver (Cleaned & Conformed):**
   * Cleans types, handles missing keys, and deduplicates records.
   * Bad records (e.g., missing business keys) are routed to a separate `_bad_rec` table with reasons logged, rather than failing the whole pipeline run.
   * Merges valid records into the Silver Delta table using SCD Type 1 (`DeltaTable.merge`) to handle updates cleanly.

3. **Gold (Aggregations):**
   * Computes daily flight metrics, carrier delays, and cancellation rates for reporting.
   * Uses partition overwrite to keep summaries up to date.

---

## Project Structure

Everything is split into small driver notebooks and a shared framework class:

```text
├── README.md
├── Framework.py       # Reusable Bronze & Silver ETL logic
├── Config.py          # Metadata config setup
├── setup.py           # Catalog / schema initialization
├── Bronze FW.py       # Bronze layer runner
├── Silver FW.py       # Silver layer runner
├── Gold.py            # Gold layer runner
└── run.py             # Entrypoint to run all layers sequentially
