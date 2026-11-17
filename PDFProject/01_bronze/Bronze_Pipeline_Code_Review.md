# Bronze Pipeline Code Review — PDFProject

**Date:** 2026-11-16  
**Environment:** Serverless compute (Spark Connect), PySpark 4.3.0.dev0, Python  
**Scope:** `01_bronze/Notebooks/01_nb_bz_pdf_process` (4 cells), `lib/common/sparkConfig.py`, `lib/common/etlConfig.py`, `lib/bronze/autoloaderconfig.py`, `lib/bronze/bronzevalidator.py`, control tables in `ai.00_etlcontrol`, output tables in `ai.02_bronze`, `lib/requirements/requirement.txt`

---

## 1. Architecture Summary

Widget-parameterized (catalog / controlTableSchema / env / source), control-table-driven bronze ingestion:

1. **Cell 1** defines widgets and reads them into Python variables.
2. **Cell 2** sets up `sys.path` and imports the local `lib` package (with `importlib.reload` for development).
3. **Cell 3** runs the pipeline: `SourceInitialize` + `AutoLoaderInitialize` load env/notebook/source/Auto Loader properties from `ai.00_etlcontrol`; `BronzeETL.bronzeLoad()` starts an Auto Loader (`cloudFiles`, binaryFile) streaming read of `/Volumes/ai/01_landing/raw/` (glob `tutorial*`), enriches rows with file-metadata columns, and processes each micro-batch via `foreachBatch` → `processBatch`: SHA-256 `documentKey`, MIME detection + PDF validation (`mapInPandas` → `Validator`/`PDFValidator`), valid rows → `bronze_tutorial_document`, invalid rows → `bronze_tutorial_rejected_Records`, one audit row per batch → `bronze_auditlog`. Checkpoint at `ai/02_bronze/tutorial/checkpoint/`; `availableNow=True` trigger with `awaitTermination()`.
4. **Cell 4** displays the three output tables.

## 2. Verified Current State (evidence gathered 2026-11-16)

| Item | Value |
| --- | --- |
| `bronze_tutorial_document` | 13 rows, 13 distinct files (valid PDFs) |
| `bronze_tutorial_rejected_Records` | 4 rows (non-PDFs: png/pptx/docx, `END OF FILE NOT FOUND`) |
| `bronze_auditlog` | 63 rows (includes historical runs) |
| Audit schema | `id bigint`; `loadid`, `batchid`, `sourcename`, `files_processed`, `valid_files`, `invalid_files`, `status`, `error_message` — all **string**; `loaddt timestamp`; `createdby string` |
| Checkpoint | Through batch 8 (loadid 20261113134612); all 17 landing files consumed — current runs are no-ops until new files land |

## 3. Findings

### Critical

**C1. `foreachBatch(self.processBatch)` — bound method capturing live Spark session objects.**
`self` (BronzeETL) holds `self.spark` (SparkSession), `SourceInitialize`, and `AutoLoaderInitialize` instances. On serverless (Spark Connect), `foreachBatch` callbacks are serialized to run server-side; the captured client-side SparkSession cannot be reconstructed there and raises `NO_ACTIVE_SESSION` on a **non-empty** batch (verified against PySpark 4.3.0.dev0: `SparkSession.__reduce__` stores only a session creator + id, which fails outside the driver). Recent runs only succeed because every batch since batch 8 has been empty and hits the `batch_df.isEmpty(): return` short-circuit — **the current code has never been proven against a non-empty batch**. The very next run after new files land in `/Volumes/ai/01_landing/raw/` is expected to fail.

*Fix:* keep the batch function free of instance state — module-level or `@staticmethod` helpers called as `BronzeETL.*`; build the closure capturing only `str`/`dict` values (e.g. `makeBatchFn(loadid, sourceName, targetProps)`); obtain Spark inside the callback from `batch_df.sparkSession`; inline the audit insert or pass only plain values to it.

### High

**H1. PDF validation re-executed up to 5× per batch (performance + cost).**
`mapInPandas` is lazy, so each downstream action re-runs full MIME detection + PDF parsing for every file: `writeBronze` (1), `writeBadRecordsBronze` (1), and `getRecordCount` — which is called **three separate times** in the `logAudit(...)` line and performs one `count()` action per call (3). That is ~5 full validations of the same files per batch.

*Fix:* compute counts once (`cnt_total, cnt_valid, cnt_invalid = self.getRecordCount(batch_df)` — a single call returns the tuple already), and/or materialize the validated batch once (`.cache()` before the writes/counts, `.unpersist()` after), or derive the three counts from one aggregation query.

**H2. Audit-log type debt.** The audit table stores `loadid`, `batchid`, and all counts as **strings** (legacy schema, confirmed via `DESCRIBE TABLE` 2026-11-16), and `logAudit` deliberately writes `str(...)` values to match. This blocks numeric filtering/aggregation on the audit trail. The commented-out `auditInsert` in `sparkConfig.py` expects proper `LongType` — code and table have drifted apart.

*Fix:* rebuild `bronze_auditlog` with `bigint` types (one-time `CREATE OR REPLACE`), update the inline insert to write numerics, and either delete the commented `auditInsert` block or restore it as the single implementation so there is one source of truth.

**H3. Wrong error-type labels in `bronzevalidator.py` `except` blocks.** `end_of_file_validation` and `file_size_validation` catch blocks return `type: "FILE OPEN VALIDATION"` — a copy/paste slip. When those checks fail via exception, `validation_checks` mislabels the root cause, which corrupts downstream error analytics.

*Fix:* return the matching type (`END OF FILE`, `FILE SIZE VALIDATION`) in each `except` block.

### Medium

**M1. Unused imports in notebook cell 2.** `pypdf` (`PdfReader`), `pymupdf` (validation lives in the lib, not the notebook), `pandas_udf`, `expr`, `current_timestamp`, `current_user`, `date_format`, `when`, `to_json`, `struct`, `Iterator`, `BinaryType`, `LongType`, `io`, and `pandas` are all imported but unused; `re` is imported twice. `pypdf` is also pinned in `requirement.txt` although no code uses it anymore — remove it or drop the pin.

**M2. Dead config.** `INLINE_THRESHOLD_BYTES` is read from source properties into `self.inline_threshold_bytes` but never used anywhere. Either implement the intended inline-vs-reference behavior or remove the read.

**M3. Unguarded `collect()[0][0]` lookups in `etlConfig.py`.** `getNotebookId()` / `getSourceId()` raise a bare `IndexError` if the notebook or source is not registered in the control tables — confusing at 2 AM. Add an existence check with a clear error message naming the missing row.

**M4. Fragile `project_root = os.path.abspath("../../")`.** Depends on the notebook's working directory; breaks if the notebook moves or is run from a Job with a different CWD. Prefer an absolute workspace path or a path derived from `dbutils.notebook.entry_point` context.

**M5. Naming typos (coordinate with control-table data before renaming):** property key `checkpint` (→ `checkpoint`) is misspelled in **both** the control-table data and the code; `getAutoloaderTagetTableProperties` / `getAutoloaderTagetPartitionColumn` (`Taget` → `Target`); `targetCatelog` (→ `targetCatalog`) in control data + code.

**M6. `zero_pages_file_open_validation` quirks.** `docs.pages` works on the installed PyMuPDF 1.28.2, but `page_count` is the canonical documented attribute — prefer it. The `str(docs.pages) == ""` condition is dead logic (never true for an int) and can be dropped.

**M7. Class-level mutable state.** `EnvConf` / `NotebookConf` hold the Spark session and config in class attributes — fine for this single-stream notebook, but not safe if cells run concurrently or the module is reused by multiple notebooks in one session. Document the single-use assumption.

**M8. `getLoadId()` collision risk.** Timestamp at second granularity (`%Y%m%d%H%M%S`); two loads started within the same second share a load id. Low probability here, but append a random suffix if load ids must be unique.

### Low / Style

- **L1.** Non-standard 8-space class-body indentation in notebook cell 3 and `autoloaderconfig.py` (methods at 8, bodies at 16/12). Works, but standardize on 4-space PEP 8 when convenient. (`sparkConfig.py` `getSourceProperties`/`getBronzeProperties` over-indented return blocks were already normalized during the 2026-11-16 docstring pass.)
- **L2.** Dead code: commented-out import at the top of cell 3, commented `withColumn` in `processBatch`, the large commented `auditInsert` in `sparkConfig.py`, commented `print` in `etlConfig.py`.
- **L3.** Lint diagnostics: `unresolved-import` on `lib.*` is a false positive (the package is resolved at runtime via `sys.path`); SCPAP005 warnings on the write paths are over-broad (`.write.saveAsTable` is an action, so those `try/except` blocks do work); SCPAP001 on `batch_df.schema` inside functions costs an extra Analyze RPC per call — cache the schema where reused. The `EXECUTED_OUT_OF_ORDER` cell warnings should be cleared by a fresh top-to-bottom run.
- **L4.** `requirement.txt` omits `pandas` and `pytz` (both are runtime-provided on serverless — fine, but document the assumption); `langchain`, `langchain-text-splitters`, `tiktoken` are unused by the bronze layer (presumably for silver — consider splitting requirements per layer).
- **L5.** `getRecordCount` uses `select("path").count()` where a plain `count()` suffices; and it is invoked 3× inline in the `logAudit` call — see H1.
- **L6.** `mapInPandas` is normally a last resort vs native expressions, but it is **justified here**: PyMuPDF and python-magic have no native Spark equivalents. Deployment note: `pymupdf`, `python-magic` (and the OS `libmagic`) must be available on the **serverless workers**, not just the client — verify they stay installed when the environment is rebuilt.

### What the code does well

- Bounded-drain streaming done correctly: valid output mode, durable `checkpointLocation`, `trigger(availableNow=True)`, and `awaitTermination()` — all four required parts present.
- Fully control-table-driven configuration (env → notebook → source → Auto Loader props) — clean, extensible pattern.
- Smart serverless workaround for the audit insert: identity columns detected from `SHOW CREATE TABLE` DDL and excluded from the insert schema.
- Rejected-records capture keeps the full `validation_checks` array-of-structs history for every bad file — good observability.
- Empty-batch short-circuit; SHA-256 `documentKey` for downstream dedup; checkpoint reuse verified working across runs.

## 4. Recommended Fix Order

1. **C1** — before any new file lands in the landing zone (otherwise the next non-empty batch fails).
2. **H1** (count once / cache) and **H3** (error labels) — small, safe edits.
3. **H2** — rebuild the audit table with proper types and align the insert (one-time migration).
4. Medium items as a single cleanup PR (imports, dead config, guards, typos).
5. Low/style items opportunistically.

## 5. Evidence Log

- `DESCRIBE TABLE ai.02_bronze.bronze_auditlog` (2026-11-16): schema as listed in §2.
- Row counts (2026-11-16): document 13 / rejected 4 / auditlog 63.
- Full successful notebook runs: 2026-11-13 (non-empty batches 0–8, loadid 20261113134612 — with the instance-free foreachBatch pattern) and 2026-11-16 (empty batches, current bound-method code).
