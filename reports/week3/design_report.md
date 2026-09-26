# Week 3 design report

## 1. Purpose and processing flow

Week 3 extends the platform from loading snapshots to processing updates. The aim is to add new records, recognise duplicates and apply corrections while keeping the existing data and Week 2 queries usable. The solution uses the same Spark and Delta Lake stack as the earlier weeks and runs as a local batch pipeline.

Each release passes through five steps: check the source schema, prepare and validate the rows, compare them with the existing Silver tables, merge the changes, and refresh the affected analytical data. Invalid rows are stored separately in quarantine. The pipeline also records counts, timings and errors so that each execution can be inspected afterwards.

The code is divided by responsibility. `generate.py` creates releases, `updates.py` validates and merges them, `refresh.py` maintains the integrated and Gold tables, and `run.py` coordinates the process. Week 1 preparation and validation functions and Week 2 analytical SQL are reused. This keeps the Week 3 changes focused on operating the existing platform rather than introducing a second implementation.

### Generating the updates

The taxi release contains new trips equal to 7.5% of the original raw row count and copied trips equal to 1.5%, rounded upward. New trips are based on valid source records. Pickup and drop-off times are shifted together, preserving trip duration, and the earliest new pickup is one hour after the original maximum. The original Parquet column types are retained. The copied rows exercise duplicate detection against data already loaded into Silver.

Weather and air-quality releases contain 48 following hours by default. Air updates include each observed NYC sensor. Weather introduces a nullable `humidity` column, validated between 20 and 100, while air quality introduces nullable `aqi`, validated between 0 and 500. The generated AQI values are synthetic test data, not a conversion from PM2.5. A zone release changes one zone label to exercise updates to reference data.

Each dataset is written to one CSV or Parquet file, with a manifest recording the expected counts and schema additions. Each release follows its own source's time coverage. Therefore, new taxi trips do not necessarily have matching weather or air observations; missing context remains null, as in Week 1. Source timestamp outliers can also shift the generated taxi release unusually far forward, so the generated dates should not be interpreted as a realistic forecast.

<!-- pagebreak -->

## 2. Incremental processing and validation

Incoming rows are compared with the current Silver table using the dataset's key. A key that does not exist is inserted. An existing key with changed values is updated. An existing key with identical values is counted as a duplicate and does not need another write. Comparisons handle null values explicitly. Rows that are absent from a release remain unchanged; a missing row is not treated as a deletion.

Weather and air observations use their existing keys, and zones use `location_id`. Taxi corrections need special handling because the original trip ID includes fare and distance. Changing either would otherwise produce a new ID. A correction must therefore provide `original_trip_id`, pointing to an existing Silver trip. The update is applied to that identity after validation. This avoids guessing whether similar trips represent the same journey.

Repeated keys within a release are resolved using the existing deterministic ordering of row values. This makes repeated executions consistent, but does not prove which conflicting row is newest. Releases should arrive in order and contain one authoritative version per key. Explicit release sequence numbers would be needed to support reliable out-of-order corrections.

Only changed rows are staged and passed to Delta `MERGE`. Staging happens before the target is modified because a comparison that reads the target could otherwise be recomputed against its updated contents. The staged rows also provide the input for identifying which analytical records need refreshing.

### Validation framework

Validation combines shared structural checks with dataset-specific rules. Shared checks cover required columns, safe type conversion, missing keys and duplicate keys. Existing Week 1 rules check taxi durations and fares, weather values and air-observation completeness. Taxi pickup and drop-off locations must also exist in the zone table. Missing environmental context is allowed, but an invalid mandatory zone reference is rejected.

Invalid rows are excluded from the merge and saved with their contents, rejection reason, run ID and timestamp. The contents are stored as JSON so that malformed rows with different schemas can share the same quarantine table. Valid rows continue through processing. A readable batch with an unsupported schema is quarantined as a whole while the other datasets can continue; an unreadable file is recorded as an execution failure.

### Schema evolution

Allowed additions and their validation rules are defined in `config/week3.yaml`. The new humidity and AQI columns are nullable, so existing rows receive null without needing fabricated historical values. If a later release omits an allowed added column, previously stored values are preserved. Original source columns remain required. Unexpected columns, removed columns and changes to existing Parquet types are rejected. CSV values are checked through casting and value rules because CSV does not carry reliable physical types.

The existing weather `rhum` field continues to supply the humidity measure used by Week 2. The new `humidity` field is stored separately, and AQI does not replace the existing PM2.5 analysis. This allows schema evolution without silently changing established query meanings. New value checks can be added as SQL predicates in configuration; new transformations, reference checks or datasets still require the corresponding code and registry changes.

<!-- pagebreak -->

## 3. Keeping analytical results consistent

After Silver is updated, the pipeline identifies trips whose integrated values may have changed. A taxi update directly identifies a trip. A zone change affects trips that use that zone at either endpoint. Weather and air updates affect trips with pickup or drop-off times in the changed observation hours. These trips are joined with the current context tables, checked for preserved trip identities, and merged into the integrated table.

The refresh then uses the existing Week 2 product definitions. Both the old and new pickup dates are collected, so moving a trip between dates also removes its contribution from the old group. The four products use the following strategies:

- **Daily mobility summary:** replace complete affected date groups.
- **Taxi-zone statistics:** replace complete affected month groups.
- **Weather impact summary:** recompute the product when relevant data changes, because shared exposure counts can affect several groups.
- **Air-quality impact summary:** recompute the product when relevant data changes, including its continuous hourly calendar.

Replacing complete groups is simpler and safer than trying to add or subtract averages. It also handles changes to zone labels and trips moving between groups. The two context summaries use full recomputation because maintaining their shared counts and calendar incrementally would require additional state and more complex correction logic.

A dependency map prevents unrelated refreshes. For example, a weather update does not require rebuilding fare and distance summaries. Replaying an unchanged release skips all product writes. Context changes with no matching trips also skip existing products under the current trip-based dependency policy. If future products expose environmental observations independently of trips, that policy will need to be extended.

Selective refresh reduces the records enriched and the groups rewritten, but it does not guarantee that Spark reads only those records. Finding affected IDs, executing joins and matching a merge can still scan large tables. This is an important distinction between the logical update strategy and its physical execution cost.

### Failure recovery and publication

Delta provides atomic commits for individual tables, but the whole release spans several Silver, integrated and Gold tables. Readers could therefore observe different stages of a release if they query while processing is still underway. The supported approach is one writer at a time, with analytical consumers waiting for successful pipeline completion.

Before making changes, the runner creates `refresh_pending.json`. A failed execution leaves this marker in place. The next successful run rebuilds integration and Gold from the current Silver state before clearing it. Recovery deliberately uses full recomputation to restore analytical consistency even if the previous run stopped halfway through publication. It does not roll back the entire release or provide a transaction across tables. A larger deployment could publish a list of committed table versions for readers to use as one consistent release.

<!-- pagebreak -->

## 4. Monitoring architecture

Operational metadata is stored in Delta alongside the data pipeline. The `executions` table records each dataset's status, schema version, processed, inserted, updated, rejected and duplicate counts, validation failures, validation time, total processing time and error message. `pipeline_runs` records overall status and phase timings. `product_refreshes` records which analytical products were refreshed and the integrated table version used. Quarantine and staged changes retain the row-level information needed to investigate a run.

The five queries in `monitoring.sql` answer the main operational questions: which datasets have validation failures most often, which take longest, how rows were handled in each execution, whether processing time changed from the preceding run, and which executions reported errors. Validation reports can also group quarantine rows by run and rejection reason to show what failed and why.

Counts need to be interpreted consistently. Unchanged rows already in Silver are duplicates but are not validation failures. Losing rows from duplicate selection within one file contribute to both duplicate and rejected counts, so those two totals should not simply be added. EPA records outside the configured scope have a separate count. Keeping these distinctions makes the monitoring results useful for explaining input handling rather than just reporting a single rejected total.

## 5. Engineering decisions and trade-offs

The design favours a small set of Python modules and explicit configuration. Reusing the earlier preparation functions and analytical SQL reduces duplicated logic and helps preserve compatibility. The main additions are correction handling, staged changes, refresh dependencies and operational records.

Duplicate checks still compare releases with existing data, corrections require stable identities, and some analytical summaries are fully recomputed. Delta history, staged changes and monitoring writes increase storage and transaction overhead. 

The main performance improvement to investigate is reducing integration reads and merge work through partition-aware matching and reuse of affected-trip IDs. Stable producer trip IDs and ordered release numbers would simplify corrections. Supporting more datasets requires their source definitions, keys, validation and analytical dependencies. Distributed scheduling and publication controls should be added only when the workload requires them, with correctness checks and measurements at representative volumes.
