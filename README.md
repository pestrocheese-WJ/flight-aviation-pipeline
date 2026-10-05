# Aviation Lakehouse Pipeline

A config-driven Medallion pipeline on Databricks that turns a year of raw U.S. flight records into clean, quality-checked tables and airline and route performance metrics.

**Stack:** Databricks (serverless) · PySpark · Delta Lake · Unity Catalog · Azure Data Lake Storage Gen2 · Databricks Jobs

| | |
|---|---|
| Source | BTS On-Time Performance, 2025 (12 monthly CSV files, 40 columns) |
| Volume | 7,001,619 flight records |
| Layers | Bronze → Silver → Gold, orchestrated as a 3-task Databricks Job |
| Load pattern | Incremental Bronze and Silver; full-refresh Gold |
| End-to-end runtime | Under 3 minutes on serverless compute |

---

## Architecture

<img width="3619" height="1579" alt="architecture" src="https://github.com/user-attachments/assets/9d8f1ff3-ba7f-4781-a527-9033c1b999e2" />

Bronze and Silver logic lives in one shared `Framework` notebook as two Python classes. Each Job task loads it with `%run` and receives the pipeline name as a task parameter, so the same code can serve any dataset that has a row in `table_config`.

---

## Config-driven design

Every setting the pipeline needs comes from a Delta table, not from code:

| Config field | Used by | Purpose |
|---|---|---|
| `pipeline_name` | both | Selects the config row (passed in as a Job parameter) |
| `file_path`, `file_format`, `header`, `delimiter` | Bronze | Where and how to read source files |
| `table_name` | both | Base name for `_bronze`, `_silver`, `_bad_rec` tables |
| `schema_details` | Silver | Column → target type, used for validation and casting |
| `keys` | Silver | Business key for duplicate detection and MERGE |
| `mode` | Silver | Write mode for the first Silver load |

The flight data uses a 6-column business key: `FlightDate`, `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Origin`, `Dest`, `CRSDepTime`.

---

## Bronze: land raw data without loading a file twice

1. Lists files in the landing folder and keeps those matching the configured format.
2. Compares them against the distinct `_file_path` values already in the Bronze table, so only new files are read.
3. Reads every column as a string (no schema inference) so nothing is lost or coerced before validation.
4. Adds lineage columns from Spark's `_metadata`: `_file_name`, `_file_path`, `_file_size`, `_file_modification_time`, plus `_load_dt` and `_load_dttm`.
5. Appends to `flights_bronze`.

The Bronze table is its own record of what has been processed: if a file's path is in the table, it has been loaded.

---

## Silver: validate, quarantine, and merge

### Incremental reads with a watermark

Silver carries each row's Bronze load time forward as `_bronze_load_dttm`. On each run it takes the latest value across both `flights_silver` and `flights_bad_rec` and reads only Bronze rows loaded after it. Checking both tables means a batch that was entirely rejected still advances the watermark.

### Row identity

Each incoming row gets two IDs:

| Column | Built from | Identifies |
|---|---|---|
| `_ck` | SHA-256 of the 6 business-key columns | The flight (shared by duplicates) |
| `_sk` | `monotonically_increasing_id()` | The individual row (unique) |

Rows are written to a staging table before validation so `_sk` stays fixed across every step that joins back on it (serverless compute does not support `.cache()`).

### Data-quality rules

Each rule is its own method that returns only the rows it rejects, with a reason:

| Reason | Rule |
|---|---|
| `_is_invalid` | A non-null value does not match its declared type (int, float, date), checked with regex before casting |
| `_is_key_null` | Any of the 6 business-key columns is null |
| `_row_duplicate` | Row is an exact copy of another row; one copy is kept |
| `_key_duplicate` | Same business key, different values; **all** copies are quarantined, because there is no reliable way to tell which one is correct |

The rejected sets are unioned into one quarantine set (a row can carry several reasons). Clean rows are found with a left anti join on `_sk`, then cast to their target types.

### Business rules

Applied to clean rows only, following the dataset's guidance to treat cancellations and diversions as separate outcomes:

- `CancellationCode` becomes `NOT_CANCELLED` for flights that operated, and `UNKNOWN` for cancelled flights with no code.
- Delay-cause columns (carrier, weather, NAS, security, late aircraft) are filled with `0` for completed flights and left `null` for cancelled or diverted flights, where a missing value means *no data*, not *no delay*.

### Writes

1. Clean rows are merged into `flights_silver` on the business key (Delta MERGE, SCD Type 1).
2. Quarantined rows are appended to `flights_bad_rec` with their reasons and raw values.

Silver is written **before** the quarantine table. If a run fails between the two writes, the good data is safe; writing in the other order could advance the watermark past rows that never reached Silver.

---

## Gold: business metrics

Rebuilt from Silver on every run (full refresh), so any correction merged into Silver flows through automatically.

| Table | Grain | Metrics |
|---|---|---|
| `gold_airline_performance_daily` | Airline × day | Total, cancelled, diverted and on-time flights; on-time %; average arrival delay; total minutes by delay cause |
| `gold_route_performance_monthly` | Route (origin → destination) × month | Total and cancelled flights; average arrival delay, air time and distance |

On-time % uses all scheduled flights as the denominator, so cancelled and diverted flights count as not on time.

---

## Results

Tested by loading 11 months, then adding the 12th:

| Run | Files | Bronze rows | Bronze | Silver | Gold |
|---|---|---|---|---|---|
| Initial load (Jan–Nov) | 11 | 6,419,315 | 36s | 1m 55s | 23s |
| Add December (+582,304 rows) | 12 | 7,001,619 | — | 55s | 15s |

- **Incremental Silver:** adding one month took roughly half the time of the initial 11-month load.
- **Reconciliation:** after the initial load, Bronze, Silver and both Gold tables each accounted for exactly 6,419,315 flights.
- **Data quality:** the source contained no rows that failed any rule. To confirm the rules actually work, they were run against a synthetic batch containing a null key, an invalid float, an invalid date, exact duplicates and conflicting duplicates; every bad row was caught with the expected reason, and valid edge cases (negative delays, `1.00` in integer columns) passed.
- **Bronze timing** stays around 40 seconds whether or not there are new files, because most of it is fixed cost (compute start, file listing, reading `_file_path`). See *Future improvements*.

---

## Lessons learned

- **`from pyspark.sql.functions import *` caused a bug that only appeared on the second run.** It replaced Python's built-in `max` with PySpark's, which broke the watermark lookup once Silver tables existed. All notebooks now import functions by name, and PySpark's `max` is aliased as `spark_max`.
- **Serverless compute does not support `.cache()`.** The row IDs needed to stay stable across several joins, so the batch is materialized to a staging Delta table instead.
- **Documented types are not file formats.** Columns described as integers (`Cancelled`, `Diverted`, `DepDel15`, `ArrDel15`) are stored as `0.00` / `1.00`. The integer rule accepts a trailing `.0`, and integers are cast through float, while true decimals like `7.5` are still rejected.

---

## Future improvements

- Replace path-based file tracking in Bronze with Auto Loader, removing the per-run scan of `_file_path`.
- Detect corrected files re-uploaded under the same name (currently they are skipped because the path already exists).
- Make Gold incremental by recomputing only the dates and months touched by each Silver run.
- Add range checks (e.g., `Cancelled` must be 0 or 1) on top of type checks.

---

## Repository structure

| Notebook | Role |
|---|---|
| `Framework` | `Bronze` and `Silver` classes, and the `get_reason` helper |
| `Bronze FW` | Job task: loads new files into Bronze |
| `Silver FW` | Job task: validates, quarantines and merges into Silver |
| `Gold` | Job task: builds the two Gold tables |

## How to run

1. Upload monthly CSV files to the ADLS Gen2 landing folder (one file per month, unique names).
2. Add a row for the pipeline to `table_config` with the fields listed above.
3. Create a Databricks Job with three notebook tasks in order: `Bronze FW` → `Silver FW` → `Gold`, and set the `pipeline_name` parameter on the Bronze and Silver tasks.
4. Run the Job. Re-running with no new files skips Bronze and Silver; adding a file processes only that file.
