<div align="center">

# Week 3 Design Report by Group 4

### Group 4: Gabor, Anggitha, Lalitha

---

</div>

## 1. Design overview

Weeks 1 and 2 established a Bronze, Silver and Gold platform for four municipal datasets: taxi trips, hourly weather, hourly air quality and taxi-zone reference data. Bronze contains the original raw snapshots, Silver holds validated and standardised records, and Gold provides reusable analytical summaries.

Week 3 adds incremental updates to this platform. New records and corrections are applied without replacing the entire Silver history. The pipeline recognises duplicates, isolates invalid input and records the outcome of each execution. Its processing flow is:

**Release files → schema checks → preparation and validation → duplicate and change detection → Silver merge → affected-trip integration → Gold refresh**

Monitoring records are written during these stages. The implementation uses Spark and Delta Lake, reusing the preparation functions from Week 1 and the analytical SQL from Week 2. Separate Python modules handle generation, updates, refresh and orchestration. The supported deployment is a local batch pipeline with one writer at a time.

## 2. Incremental processing

A release is generated for each dataset. Taxi updates use Parquet; weather, air quality and zones use CSV. New observation timestamps follow each dataset's original coverage. The datasets end at different times, so new taxi trips may have no matching environmental observations. Such context remains null under the existing integration policy.

| Dataset | Release contents | Outcome in each full-data evaluation run | Schema addition |
|---|---|---|---|
| Taxi trips | New trips and copies of existing trips | 859,931 processed; 716,609 inserted; 143,322 duplicates | None |
| Weather | 48 following hourly observations | 48 inserted | Nullable numeric `humidity` |
| Air quality | 48 following hours for each observed NYC sensor | 288 inserted | Nullable numeric `aqi` |
| Taxi zones | One corrected zone label | One existing row updated | None |

The taxi generator creates new rows equal to 7.5% of the original raw count and duplicates equal to 1.5%, rounded upward. It shifts pickup and drop-off timestamps together so that durations are preserved, retains the source column types, and places the earliest new pickup one hour after the original maximum. Selected source attributes and valid ranges are retained, but the generated release is not claimed to preserve the full statistical distribution. Air-quality AQI values are synthetic test values rather than a conversion from PM2.5.

### Merge behaviour and record identity

After validation, records are compared with the current Silver table using the dataset key and a null-safe comparison of values. Unseen keys are inserted, matched keys with changed values are updated, and unchanged matches are counted as duplicates. Records absent from a release remain unchanged. Reprocessing the same valid release therefore creates no additional copies of its records.

Weather and air use their existing observation keys, while zones use `location_id`. Taxi corrections require an explicit `original_trip_id` referencing an existing Silver trip. This is necessary because the original taxi hash includes fare and distance: changing either would otherwise create a different ID. The correction is validated and applied to the referenced identity rather than matched approximately to a similar journey.

Repeated keys within one release are resolved using the existing deterministic ordering of row values. This produces repeatable results but does not establish which conflicting version is newest. Releases should arrive in order and contain one authoritative version per key. Producer sequence numbers would be needed to handle out-of-order corrections reliably.

Changed rows are staged before the Delta merge. This preserves the change set even if Spark recomputes a comparison after the target table has changed. Those staged rows also drive analytical refresh. Run IDs and source paths provide traceability; they do not prevent replay. Replay safety comes from record-level comparison with Silver.

Week 3 reads the incremental release files directly and does not append them to Bronze. Existing Bronze snapshots remain unchanged, while source release files, staged changes and execution records provide evidence about the update. An automatic archive of every incoming release is not implemented.

### Schema evolution

Allowed additions are defined in `config/week3.yaml` with their types and SQL validation predicates. Humidity must be null or between 20 and 100, and AQI must be null or between 0 and 500. Merge-level schema evolution introduces these nullable fields, leaving historical values null. A subsequent release that omits an allowed added field preserves its existing stored values.

Original source columns remain required. Unexpected additions, removed columns and changes to existing Parquet types are rejected. CSV does not provide reliable physical type information, so casting and value rules check its contents. Changes to units, definitions or key meaning require manual review and, where necessary, migration; the validator cannot automatically detect a semantic change that retains the same schema.

The existing weather `rhum` field continues to supply the humidity measure used by the Week 2 queries. The new `humidity` field is retained separately, and AQI does not replace PM2.5 in existing analyses. This makes the added attributes available without changing established analytical meanings.

## 3. Analytical consistency

The platform retains the four Week 2 Gold products and their established definitions. Refresh begins by identifying affected trips: taxi changes identify trips directly, zone corrections identify trips using that zone at either endpoint, and weather or air changes identify trips with endpoints in the affected hours. These trips are enriched with current context and merged into the integrated Silver table, with checks that trip identities are preserved.

Both old and new pickup dates are collected. A correction that moves a trip to another date must remove its contribution from the old group as well as add it to the new one. Gold uses complete-group replacement or full recomputation according to the product:

| Gold product | Refresh strategy |
|---|---|
| `daily_mobility_summary` | Replace complete affected date groups |
| `taxi_zone_statistics` | Replace complete affected month groups |
| `weather_impact_summary` | Recompute when relevant data changes because exposure counts are shared across groups |
| `air_quality_impact_summary` | Recompute when relevant data changes, including its continuous hourly calendar |

Complete-group replacement avoids trying to update averages by simple addition and handles obsolete groups after date or label corrections. It uses scoped overwrites, not row-by-row Gold merges. The environmental summaries use full recomputation to avoid maintaining additional shared counts and calendar state.

A dependency map skips unrelated products. An unchanged replay skips all product writes. Under the current trip-based policy, context changes with no matching trips also skip existing products. Products that later expose environmental observations independently of trips would need broader dependencies.

Selective refresh reduces enrichment and output replacement, but does not guarantee proportionally smaller input reads. Finding affected trips, joining context and matching merges may still access large tables. The full-data evaluation compared the resulting products across processing configurations; it did not independently compare them with a complete rebuild. That separate comparison was performed in the 200-trip correctness fixture.

### Recovery and publication

A Delta commit is atomic for one table. An entire release updates several tables and is not atomic across Silver, integration and Gold. Analytical consumers should therefore wait for successful pipeline completion, and only one writer should apply releases at a time.

Before changes begin, the runner creates `refresh_pending.json`. If execution fails, the marker remains. The next successful invocation rebuilds integration and Gold from current Silver before clearing the marker. Recovery uses full recomputation to restore analytical consistency; it does not roll back all Silver changes from an interrupted release. A future deployment could publish committed table versions as a consistent release for readers.

## 4. Monitoring architecture

Operational metadata is stored in Delta. The `executions` table records each dataset's status, schema version, processed, inserted, updated, rejected and duplicate counts, scope exclusions, validation failures, validation time, processing duration and error message. `pipeline_runs` stores overall status and phase timings. `product_refreshes` records analytical refresh history and the integrated table version used.

The five queries in `monitoring.sql` report validation-failure frequency, average and worst dataset durations, counts per execution, duration changes between runs, and errors from unsuccessful executions. The `monitor` command prints these query results; the underlying records are already persisted during processing. Quarantine can also be grouped by run and rejection reason to produce a validation report.

Counts must be interpreted consistently. Unchanged Silver matches are duplicates but not validation failures. Losing rows from duplicate selection within a release contribute to both duplicate and rejected counts, so those totals overlap. EPA records outside the configured scope are counted separately. These distinctions allow an execution to be explained without treating every excluded row as invalid.

## 5. Validation framework

Validation runs before records are merged into Silver. Valid rows continue; invalid rows are stored in a dataset-specific quarantine table. Each quarantine entry contains a JSON `record`, `_rejection_reason`, `run_id` and `rejected_at`. The JSON contains the available row fields and validation information. The dataset is identified by the table path, while the execution record stores the source path. These are not all separate quarantine columns.

| Check | Implementation |
|---|---|
| Duplicates | Dataset keys identify repeated incoming rows and unchanged Silver matches |
| Invalid attributes | SQL predicates check taxi durations, monetary values and environmental ranges |
| Incomplete records | Required-column checks, safe casts and dataset rules detect missing or invalid required values |
| Missing references | Taxi pickup and drop-off locations are checked against the current zone table |
| Unsupported schemas | Source-column comparison and the configured additions identify missing columns, unexpected fields and Parquet type changes |

Rules are named SQL predicates in the dataset configuration. Added columns have configured types and predicates. Adding a range check can therefore be done without changing orchestration code. There is no general severity/action rule engine. Dataset-specific transformations and complex reference checks remain in Python, and registering a new dataset requires its source contract, keys and analytical dependencies.

Record-level failures are quarantined while valid rows continue. A readable release with an unsupported schema is quarantined as a batch before its Silver merge, while other datasets can continue. An unreadable file is recorded as an execution failure. Missing weather or air matches are permitted context gaps, rather than mandatory-reference failures.

## 6. Engineering decisions and trade-offs

The design favours explicit configuration and a small set of modules. Reusing earlier preparation and analytical SQL keeps the implementation understandable and reduces duplicate logic. The main new responsibilities are correction identity, staged changes, refresh dependencies, recovery and operational records.

This simplicity has costs. Duplicate detection still reads existing data, some summaries are fully recomputed, and per-table Delta writes add overhead. Historical files, staged changes and monitoring records also accumulate. Retention and compaction would be needed for sustained operation, but automatic deletion is not part of this implementation.

Integration is the main measured performance bottleneck. Partition-aware merge matching and reuse of affected-trip IDs are candidates for reducing work, but require correctness checks and new measurements. Stable producer trip IDs and ordered release numbers would simplify corrections. Supporting additional datasets requires their schemas, validation and dependencies to be defined explicitly.

The evaluation report separates repeated full-data update measurements from the small correctness fixture. Successful comparisons across configurations show that disabling validation or monitoring did not change product contents for the valid release. The fixture supplies targeted evidence for invalid inputs, corrections, recovery and agreement with full recomputation. Neither establishes distributed scalability. More infrastructure should follow a demonstrated workload need rather than be added solely to increase architectural complexity.
