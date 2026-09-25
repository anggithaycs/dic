# Urban Data Platform: Weeks 1, 2 and 3

This project uses Apache Spark and Delta Lake to analyse New York taxi trips alongside weather, air quality and taxi-zone data.

- **Week 1:** ingest and validate four datasets, integrate trips with environmental and location information, and benchmark three storage layouts.
- **Week 2:** answer six analytical questions, build four reusable Gold summaries, and test caching, partition pruning, broadcast joins and Adaptive Query Execution (AQE) on all six queries.

- **Week 3:** generate incremental releases, merge updates, evolve schemas, refresh affected analytics, quarantine bad records and measure operating costs.

## Find the right guide

| What you need | Where to go |
|---|---|
| Week 3 commands and operating policy | [Week 3 guide](src/week3/README.md) |
| Week 3 plan and reports | [Week 3 reports](reports/week3/README.md) |
| Project setup and Week 1 execution | Continue below. |
| Week 1 reports and evidence | [Week 1 reports README](reports/week1/README.md) |
| Week 2 design and benchmark reports | [Week 2 reports README](reports/week2/README.md) |
| Week 2 commands and code walkthrough | [Week 2 code README](src/week2/README.md) |
| Analytical queries and SQL organization | [Week 2 SQL README](src/week2/sql/README.md) |

## Setup and execution

The platform requires Python 3.10–3.12 and Java 17 or 21. From the project root, create a virtual environment, install the pinned runtime dependencies, and set the Java and Python paths before running the pipeline:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:/path/to/jdk-21'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
.venv/Scripts/python.exe scripts/run_week1.py
```

On Windows, Spark also needs compatible `winutils.exe` and `hadoop.dll` files in `HADOOP_HOME/bin`. The session factory detects `C:/hadoop` automatically when it exists. The first run needs network access to download the Delta JVM dependencies, and Java must be able to write to its user dependency cache.

Download the Q1 2024 yellow taxi Parquet files, `weather.csv`, `hourly_88101_2024.csv`, and `taxi_zone_lookup (1).csv` using the assignment links, then place them in `data/`. Their paths are defined in `config/platform_config.yaml`. The command-line runner resolves relative paths from the project root.

The default command ingests all four sources, builds the integrated taxi table, and benchmarks three taxi storage layouts. It also saves the measurements as JSON and regenerates `reports/week1/benchmark_report.md`. To rebuild the ingested and integrated tables without running the benchmark, use:

```powershell
.venv/Scripts/python.exe scripts/run_week1.py --skip-benchmark
```

Both commands replace the Bronze and Silver snapshots. A full benchmark writes its layouts into fresh directories so files from previous runs do not affect the measurements. By default, Spark uses `local[*]`, requests `8g` of driver memory, and uses eight shuffle/writer partitions. Each benchmark query has five measured repetitions. Allow enough memory and disk space for the materialized source data, quarantined rows, integrated output and benchmark copies.

## Run Week 2

After Week 1 completes successfully, run the full Week 2 workflow from the project root using the same environment:

```powershell
.venv/Scripts/python.exe -m src.week2.run all --repeats 5 --month 2024-02
```

This builds Gold, saves all six Silver query answers and runs the benchmark suite. If the Week 1 Silver tables already exist and are current, you can start here. Otherwise, run Week 1 first; its `--skip-benchmark` option prepares the data without running the Week 1 storage experiments.

For individual stages:

| Task | Command |
|---|---|
| Run all six Silver queries | `.venv/Scripts/python.exe -m src.week2.run queries` |
| Run one query | `.venv/Scripts/python.exe -m src.week2.run queries --query monthly_zone_demand` |
| Build or refresh Gold | `.venv/Scripts/python.exe -m src.week2.run products` |
| Query existing Gold products | `.venv/Scripts/python.exe -m src.week2.run queries --gold` |
| Rebuild Gold and benchmark | `.venv/Scripts/python.exe -m src.week2.run benchmark --repeats 5 --month 2024-02` |

The six analyses cover monthly zone demand, distance by weather, air quality and demand, weather-related demand variation, weekday peak hours and monthly demand trends.

The benchmark includes six Silver/Gold comparisons and **24 optimization comparisons**, testing all four Spark techniques on every query. It measures **48 cases**, each with one warmup and five timed repetitions by default. Answers are checked for equivalence, and executed plans are saved for inspection.

`--month` selects the pruning experiment's window, not the period for the whole suite. Five pruning queries use that month; monthly trends also reads the preceding month. Use `--repeats 1` for a smoke check. `--query` selects an analytical answer but does not narrow the benchmark.

### Week 2 outputs

```text
storage/delta/gold/
  daily_mobility_summary/
  taxi_zone_statistics/
  weather_impact_summary/
  air_quality_impact_summary/
  _metadata/

storage/metrics/week2/runs/<execution_time>/
  <query_name>.json   # Complete answers when the queries stage runs
  products.json      # Product sizes and build times when Gold is built
  metrics.json       # Benchmark status, samples, comparisons, SQL and plans
  plans/             # Before/after plans grouped by technique and query
```

The terminal prints the exact run directory. Confirm that `metrics.json` reports success before using a complete run in a report. Gold refreshes replace the current snapshots; rebuild after changing Silver or SQL. Week 2 reports are maintained manually under [reports/week2](reports/week2/README.md); the analytical runner does not generate or overwrite them.

## Code organization

The source code follows the processing stages. `src/common` validates configuration, sets up logging and creates Spark sessions. `src/ingestion` implements the shared read, prepare, classify and publish lifecycle, together with the four domain transformations. `src/integration` joins zone, weather and air-quality context and verifies trip identity. `src/storage` runs independent experiments from raw input and measures query performance.

Week 2 lives in `src/week2`: `analysis.py` prepares shared views, `products.py` builds Gold, `benchmark.py` measures and validates comparisons, and `run.py` provides the command-line entry point. Substantial SQL definitions live under `src/week2/sql`.

The `scripts` directory contains orchestration and report-generation entry points. The `tests` directory holds fast configuration and orchestration checks, isolated Spark fixtures and post-run acceptance checks. Week 1 reports and exports are in `reports/week1`; Week 2 design and benchmark reports are in `reports/week2`.

## Configuration and contracts

Each dataset specification defines its source and output paths, file format, required columns, rename and cast mappings, keys, SQL validity rules, quality thresholds, timezone and partition columns. Cast mappings refer to required source columns by their standardized names. For weather, air quality and zones, `selected_columns` defines the clean output schema after validation; a missing requested column causes an explicit failure. Transformations that depend on the meaning of a dataset remain in Python.

The weather CSV does not declare its timezone, so `datasets.weather.timezone` defaults to UTC. This assumption should be checked against the export settings. Validation-rule placeholders obtain their values from the dataset’s `quality_rules`, except `{source_timezone}`, which comes from its `timezone`. The shared ingestion audit is written to `paths.metadata_path`. Setting `partition_cols: []` leaves a table unpartitioned, including the integrated table.

Before starting Spark, the runner validates the configuration. It then resolves the source schemas, validation rules, output schemas and integration schema before writing any table. This catches unsupported dataset names, missing Week 1 sources, invalid placeholders, missing casts or output columns, and invalid partition columns early. Errors encountered while reading or executing the data can still occur later. Each table commits separately, so a failed run can leave tables at different refresh stages.

When constructing an ingestor directly, provide `dataset_spec`, `schema_version` and `metadata_path`; there is no alternate API accepting separate path or threshold keywords. The ingestor copies the specification so later changes by the caller cannot alter it. During a runner execution, ingestion audit records and the JSON summary share one run ID. Standalone ingestor runs generate their own IDs.

## Output and data semantics

The original input files remain unchanged. Bronze stores source fields with sanitized names and an ingestion timestamp. Silver stores clean source tables and integrated trips as Delta snapshots. Each dataset also has an append-only quarantine table containing invalid and duplicate rows, their rejection reasons, run ID, schema version and rejection time. Successful ingestions append audit records with counts, paths, duration and completion time. The recorded duration excludes the final audit write.

Ingestion materializes both the raw input and a classified result on disk so counts and writes can reuse the same records. Every successful ingestion verifies `initial_records = valid_records + rejected_records + out_of_scope_records`. Rejected records include duplicates, while the invalid-record count excludes duplicates. Air observations outside NYC are counted separately as out of scope. When valid rows share a key, deterministic lexicographic ordering of their payloads selects the survivor. This removes duplicates within a snapshot but does not resolve later corrections.

Taxi timestamps are converted to UTC, while pickup date, year, month, day and hour retain their New York calendar meaning. EPA timestamps use the source GMT fields. Missing passenger counts default to one, and missing tips and tolls default to zero. Malformed values still cause rejection even if a later default could replace them. Total amounts, tips and tolls must be finite and nonnegative.

Weather observations can remain null. When present, precipitation must be finite and nonnegative, pressure must be finite and positive, cloud cover must be between 0 and 8, and the weather code must be between 1 and 27. The YAML configuration defines the remaining bounds. Zone records with blank borough labels are rejected.

Integration attaches context to pickup and dropoff using the UTC hour containing each timestamp. Missing location labels become `Unknown`, while unavailable precipitation and pollution measurements remain null. The `pickup_weather_matched` and `dropoff_weather_matched` flags, precipitation-missing flags and PM2.5 source labels expose gaps in coverage. Before publication, integration verifies unique, non-null trip IDs and exactly matching input and output ID sets.

`pickup_pm25_borough` holds the same-hour borough estimate and remains null when local coverage is unavailable. `pickup_pm25_citywide` holds the same-hour mean across available NYC sites, so its value is identical for every trip in that hour. The convenience field `pickup_pm25` prefers the borough estimate, then the citywide estimate; `pickup_pm25_source` records that choice. Equivalent fields describe dropoff conditions.

Site identifiers are required because the calculation first averages instruments within each site, then weights sites equally. Use the citywide series for citywide analysis. For borough comparisons, use the borough series and report its coverage alongside the results.

Read outputs by Delta path, for example:

```python
from src.common.spark_session import get_spark_session

spark = get_spark_session()
trips = spark.read.format("delta").load("storage/delta/silver/integrated_taxi_trips")
trips.groupBy("pickup_borough").count().show()
spark.stop()
```

## Checks and reports

```powershell
.venv/Scripts/python.exe tests/test_configuration_and_runner.py
.venv/Scripts/python.exe tests/test_week1_regressions.py
.venv/Scripts/python.exe tests/test_week1_acceptance.py
.venv/Scripts/python.exe tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff check src scripts tests
.venv/Scripts/python.exe -m ruff format --check src scripts tests
```

The configuration suite runs without a Spark session. Spark regression fixtures write only to `.test-output/`, including their audit records. Run the acceptance suite after a full data run because it reads the configured Delta outputs. Before running Ruff, install the pinned development dependencies with `pip install -r requirements-dev.txt`. The lint and formatting rules are in `pyproject.toml`.

Each Week 1 execution saves a timestamped JSON summary and atomically replaces the latest-summary file. An artifact-writing failure causes the command to exit with a failure status; if processing had already failed, the original exception is preserved. Ingestion-only runs also replace the latest summary, so select a timestamped successful full run when regenerating a benchmark report:

```powershell
.venv/Scripts/python.exe -m src.storage.report --metrics storage/metrics/<successful-full-run>.json
```

The renderer requires successful measurements from raw taxi input, with all three layouts present and equivalent query results. It presents layouts in an explicit order and reads query statistics from a single shared structure. Fresh benchmark directories prevent obsolete files from affecting results. The JSON retains timing samples and physical plans for inspection. Interpret the measurements with their limits in mind: queries run on a warm system, and each layout has only one ingestion trial in a fixed order.

The editable deliverables are in `reports/week1/`. The PDF builder reads these Markdown sources directly and has no build side effects when imported. To rebuild PDFs later, use a separate artifact environment with ReportLab, pypdf, pdf2image, Pillow and Poppler:

```powershell
python scripts/package_week1.py --metrics storage/metrics/<successful-full-run>.json
```

Set `POPPLER_BIN` if Poppler is not on `PATH`. The builder requires an explicitly selected successful raw-input metrics artifact. It regenerates the benchmark source and copies the JSON to `reports/week1/evidence/`. Visually inspect any PDFs rebuilt after source edits. The Spark runner itself regenerates only benchmark Markdown. Older benchmark and validation material is historical; consult the canonical benchmark report to identify the measurements currently presented.

## Run Week 3

After Week 1 and Week 2 Gold are available:

```powershell
.venv/Scripts/python.exe -m src.week3.run generate
.venv/Scripts/python.exe -m src.week3.run evaluate --repeats 3
.venv/Scripts/python.exe -m src.week3.run update
.venv/Scripts/python.exe -m src.week3.run monitor
```

Evaluate before updating the baseline so each independent copy processes new records. Full-data evaluation creates several Delta copies and needs additional disk space. For a small, self-contained evaluation, use `.venv/Scripts/python.exe tests/test_week3.py --evaluate`.

The [Week 3 guide](src/week3/README.md) explains schema evolution, corrections, selective refresh, quarantine, recovery, monitoring and measurements. The [implementation plan and reports](reports/week3/README.md) map these features to all five assignment tasks.

## Next tasks

Week 4 can reuse preparation and integration while adding features and temporal splits for each prediction target.
