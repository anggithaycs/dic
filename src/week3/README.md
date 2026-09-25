# Week 3 operation

Run commands from the project root with the Python and Java environment described in the main README. Complete Week 1 and build Week 2 Gold first. The existing raw files and source schemas are used as the contract for each update.

## Normal workflow

```powershell
.venv/Scripts/python.exe -m src.week3.run generate
.venv/Scripts/python.exe -m src.week3.run update
.venv/Scripts/python.exe -m src.week3.run monitor
```

Generation creates a new directory, `data/week3_release`, containing four physical files and `manifest.json`. It refuses to replace an existing release. Use `--updates data/another_release` for a new one. Default taxi counts are ceilings of 7.5% new and 1.5% duplicate rows relative to the raw input count. Weather and air each cover 48 following hours; air includes each observed NYC sensor. The zone release contains one label correction.

The datasets have different original time coverage. Each release follows its own source's final timestamp; this does not imply that all new taxi trips have environmental coverage. Missing context remains null under the Week 1 policy. Taxi timestamp outliers in the original file can move the synthetic release far into the future; inspect the manifest and source maximum before interpreting this as a realistic calendar simulation.

An update directory may contain any subset of the four named files. Update processing changes configured Silver tables and Gold. It does not rebuild Bronze snapshots. Replaying the same release makes no further Silver data changes and skips Gold refresh. Keep one writer active at a time and apply corrections in release order.

## Schema evolution and corrections

`config/week3.yaml` explicitly permits numeric `humidity` in weather and `aqi` in air. These are nullable additions; historical rows receive null. Existing `rhum` still supplies the Week 2 humidity field. The separate new humidity column is retained without silently changing older analytical definitions.

Add a nullable source column by defining its type and SQL validity predicate in `additions`. Existing source columns remain required. Renames, removals, unknown columns and Parquet type changes are rejected at batch level. CSV values are checked with safe casts and the existing rules. Approve changes to meaning, units or keys manually and migrate historical data explicitly.

Weather, air and zone corrections use existing keys. A taxi correction must include a string `original_trip_id` referencing an existing Silver trip; all normal raw columns must still be present. Without it, changing fare or distance creates a different hash and is treated as a new trip. Do not infer identities from similar timestamps.

## Monitoring and validation reports

`monitor` executes `monitoring.sql`: failure frequency/rate, average and worst duration, counts per execution, duration trends, and errors. Operational Delta tables are under `storage/delta/week3`:

- `executions`: per-dataset counts, schema version, status, validation time and processing time.
- `pipeline_runs`: overall success/failure and phase timings.
- `quarantine/<dataset>`: JSON row envelopes, reasons, run ID and rejection timestamp.
- `product_refreshes`: analytical refresh history and source Delta version.
- `impacts/<run_id>/<dataset>`: changed rows used for refresh and inspection.

Read quarantine with Spark, then group by `_rejection_reason` and `run_id` to produce a validation report:

```python
spark.read.format("delta").load("storage/delta/week3/quarantine/taxi_trips").groupBy(
    "run_id", "_rejection_reason"
).count().show(truncate=False)
```

Duplicate counts include valid keys repeated within a release and unchanged rows already in Silver. Rejected counts include within-file duplicate losers, while validation failures exclude those duplicates. Out-of-scope EPA rows have their own count. Do not add duplicate counts to rejected counts: they overlap for within-file duplicates.

A failed execution leaves `refresh_pending.json`. Fix the input or runtime error and rerun the update with the same operational directory. The next completed run rebuilds analytical state from current Silver, then clears the marker. Do not delete the marker manually. There is no transaction across all datasets or products: analysts requiring a consistent release should read after successful pipeline completion.

## Tests and evaluation

```powershell
.venv/Scripts/python.exe tests/test_week3.py
.venv/Scripts/python.exe tests/test_week3.py --generation-only
.venv/Scripts/python.exe tests/test_week3.py --evaluate
.venv/Scripts/python.exe -m src.week3.run evaluate --repeats 3
.venv/Scripts/python.exe -m ruff check src/week3 tests/test_week3.py
.venv/Scripts/python.exe -m ruff format --check src/week3 tests/test_week3.py
```

The generation-only check uses the source Parquet timestamp-without-timezone type and checks schema preservation and strict timestamp bounds.

The test builds a 200-trip fixture under a unique `.test-output/week3-*` directory. Its evaluation option runs three repetitions of each ablation on independent fixture copies. The CLI evaluation copies the configured full Silver and Gold baseline for each case; allow substantial disk space. Run it BEFORE applying that release to the baseline, otherwise the experiment measures replay.

Evaluation saves raw samples and a summary under `storage/metrics/week3/<id>`. It compares normal processing, processing without the additional row validation, and processing without operational monitoring. Schema gates, casting, key checks and deduplication remain enabled in all cases. Use valid releases so validation ablations have equivalent semantics. Invalid inputs belong in correctness tests.

The measured storage increase includes retained obsolete Delta files, logs, impact tables and monitoring, and also the added source records. It is physical operational growth, not solely monitoring overhead. No automatic VACUUM or deletion is performed. Rotate case order over repetitions; medians can still be noisy on a local machine. A negative overhead is measurement noise, not evidence that a feature has negative cost.

## Code map

`generate.py` generates releases; `updates.py` validates and merges; `refresh.py` maintains integration and Gold; `run.py` orchestrates; `evaluate.py` clones and measures cases. Week 2 analytical SQL is reused unchanged. Read the plan and reports under `reports/week3`.
