# flight-aviation-pipeline


This project demonstrates a aviation data pipeline built on the **Databricks Lakehouse Platform** and managed via **Unity Catalog**. 

The pipeline ingests raw, semi-structured flight logs from an **Azure Data Lake Storage (ADLS Gen2)** landing zone through Managed Volumes. Employing a strict **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**, it implements an in-place file tracking mechanism for incremental ingestion and enforces a **Quarantine Pattern (`_bad_rec`)** to isolate corrupt schema records without breaking the pipeline. Validated records undergo SCD Type 1 upserts in the Silver layer and are aggregated into analytics-ready **Gold Delta Tables**, delivering high-performance operational metrics for flight delays and carrier performance. The entire workflow is fully orchestrated as an idempotent multi-task DAG using **Databricks Job**.

---

## Architecture & Data Flow
<img width="1111" height="637" alt="{EEC2F490-8EE7-491D-9964-3EA637FCFB33}" src="https://github.com/user-attachments/assets/563791ad-0e41-43c0-9f06-311c063a7c6f" />

Orchestrated via Databricks Workflows (`Flight job`):
<img width="1211" height="438" alt="image" src="https://github.com/user-attachments/assets/f5115530-f6ae-4e2b-9cac-569f8b95bd39" />

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
