# Task 3 — Platform Monitoring System

## 1. Objective

Task 3 adds execution-level operational monitoring to the Week 3 production pipelines. The reusable monitoring layer covers Taxi, Weather, and Air Quality incremental ingestion; the integrated Taxi refresh; and the four Gold analytical product refreshes: Daily Mobility Summary, Taxi Zone Statistics, Weather Impact Summary, and Air Quality Impact Summary.

The full-refresh benchmark is deliberately excluded from operational monitoring because it is an evaluation workload, not a production refresh.

## 2. Monitoring Architecture

```text
Pipeline execution
        |
        v
PipelineMonitor
        |
        v
Delta table: storage/delta/metadata/pipeline_runs
        |
        v
Spark SQL monitoring queries
        |
        v
JSON evidence and operational analysis
```

`pipeline_runs` is append-oriented and has one consistent, nullable schema for heterogeneous pipeline types. It records a pipeline identity, timestamps, execution outcome, row metrics, schema and Delta-version metadata, source/target paths, and optional failure diagnostics.

The monitor isolates its own Delta-write failures: a monitoring failure is logged and does not mask the underlying pipeline outcome. The API also supports failed-execution records with `error_type` and `error_message`. Fields that do not apply to a specific execution remain `NULL`; they are not fabricated.

## 3. Metrics Recorded

The required operational metrics are execution time, processed records, inserted records, rejected records, schema version, and validation failures. The monitoring schema also retains updated records, duplicate records, pipeline/dataset or product identity, status, start/end timestamps, Delta versions, schema changes, input/target paths, and failure diagnostics.

The monitoring wrappers consume metrics already produced by the existing pipelines. They do not replace validation, transformation, or Delta `MERGE` logic.

## 4. Historical Monitoring Population

The initial monitoring history was backfilled from already measured Week 3 execution evidence rather than rerunning production pipelines. The backfill uses deterministic historical pipeline IDs, so rerunning it does not duplicate records.

The populated table contains **8** successful executions:

| Pipeline kind | Rows |
|---|---:|
| `incremental_ingestion` | 3 |
| `integration` | 1 |
| `gold_refresh` | 4 |
| **Total** | **8** |

The backfill and monitoring-query checks confirmed that the relevant production Delta versions and existing Task 1/Task 2 evidence remained unchanged. The monitoring-query evidence is available in [`monitoring_queries.json`](../../storage/metrics/week3/monitoring_queries.json).

## 5. Monitoring Query Results

The four required analyses are Spark SQL queries over a temporary view of `pipeline_runs`.

### 5.1 Dataset with Most Validation Failures

`air_quality` has the highest recorded validation-failure count: **362,187**.

This is the available pipeline-level rejected/validation-failure record count. The current historical pipeline does not expose per-rule validation-failure counts, so this result does not claim a per-rule diagnosis.

### 5.2 Longest Processing Time

`integrated_taxi_trips` was the longest recorded execution at **344.928 seconds**. It processed and inserted **659,476** new integrated Taxi records. This was longer than the individual incremental-ingestion and Gold refresh executions in the current history.

### 5.3 Rejected Records per Execution

| Dataset/product | Rejected records |
|---|---:|
| `air_quality` | 362,187 |
| `taxi_trips` | 11,256 |
| `weather` | 0 |
| `integrated_taxi_trips` | 0 |
| Gold products | `NULL` / not applicable |

`NULL` for Gold products means the existing Gold refresh logic does not expose a rejected-record concept. It does not mean zero records were rejected.

### 5.4 Processing Time Trend

The chronological measured execution history is:

| Execution | Duration (s) |
|---|---:|
| Taxi incremental ingestion | 51.573 |
| Weather incremental ingestion | 14.298 |
| Air Quality incremental ingestion | 32.651 |
| Integrated Taxi refresh | 344.928 |
| Daily Mobility Summary | 4.225 |
| Weather Impact Summary | 10.249 |
| Taxi Zone Statistics | 3.391 |
| Air Quality Impact Summary | 3.202 |

This is an execution history, not a statistically meaningful long-term trend: only one historical execution per monitored pipeline/product is currently available. It does not establish performance improvement or degradation over time.

## 6. How Monitoring Helps Debugging and Maintenance

The table makes high rejected-record counts visible alongside the pipeline that produced them, separating duplicate and validation-rejection counts where the source pipeline supplies both. Execution duration identifies slow runs, while pipeline/product identity, input/target paths, Delta versions, and schema changes make a record traceable to its published data state.

For future failed runs, `status`, `error_type`, and `error_message` provide a direct starting point for investigation. Repeated records will also enable comparisons with historical behavior without changing the existing Task 1 or Task 2 business logic.

## 7. Useful Metrics for Production

The most useful implemented operational signals are execution duration; processed, inserted, and rejected counts and their ratios; validation failures; duplicate counts; status and failure diagnostics; schema changes; and Delta versions. Together they show whether a run completed, what volume it handled, whether data quality changed, whether the table shape evolved, and which exact Delta state was produced.

## 8. Additional Production Monitoring

The following are future production improvements, not implemented Task 3 features:

- Per-validation-rule failure counts.
- SLA/runtime thresholds and alerting.
- Data freshness and lateness monitoring.
- Row-volume anomaly detection.
- Storage-growth monitoring.
- CPU, memory, and executor utilization metrics.
- Retry counts and downstream-dependency failure tracking.
- Dashboard and alert integration.

## 9. Limitations

- Only eight historical executions are currently monitored.
- All historical records are successful, so failed-run querying has not yet been demonstrated with a real failure.
- Per-rule validation failure counts are not currently exposed.
- Some Gold metrics, including rejected and duplicate rows, are not meaningful or available and remain `NULL`.
- Historical rows were backfilled from previously recorded execution evidence; future executions are captured by the automatic monitoring wrappers.

## 10. Conclusion

Task 3 provides a reusable operational monitoring layer over the Week 3 pipelines using Delta Lake and Spark SQL. It captures execution metadata in a consistent append-oriented table, supports historical operational analysis, and preserves the existing Task 1 and Task 2 business logic.
