# Operating the urban data platform
## Scope and architecture

Week 3 extends the existing Spark and Delta platform with incremental releases, schema evolution, selective analytical refresh, monitoring and validation. The design uses a sequential batch command and ordinary Python modules. This is sufficient to demonstrate the operating principles on one machine without adding a scheduler, service layer or message broker.

The processing path is release files, schema checks, reusable preparation and validation, a changed-row comparison, Silver MERGE, affected-trip enrichment and Gold refresh. Invalid records take a separate path to quarantine. Execution and refresh metadata are appended to Delta tables. The original raw sources remain unchanged and continue to define the source contract.

The implementation reuses Week 1 naming, timestamp conversion, business keys, dataset transformations and SQL predicates. Week 2 queries and product SQL remain unchanged. Shared code performs loading, casting, duplicate selection, merging, audit writes and orchestration. Domain code remains responsible for taxi identities, sensor keys, borough mapping and context joins.

## Simulated releases

Taxi generation copies valid source records, shifts pickup and dropoff times by the same interval, and retains distances, locations, fares and source column types. It generates 7.5% new records and includes 1.5% original duplicate records, rounded upward. The earliest new pickup is one hour after the raw source's maximum pickup. Preserving elapsed source intervals keeps durations realistic, although a pathological source maximum can produce an unrealistic release date.

Weather generation starts at the next valid source hour and produces 48 observations by default. It retains valid meteorological values, varies temperature smoothly and adds numeric humidity between 55 and 79 percent. Air generation retains each NYC sensor's latest valid attributes, produces consecutive hours after the dataset's last valid observation, and varies PM2.5 and synthetic AQI within permitted ranges. AQI is demonstration data rather than a scientific conversion from PM2.5.

A fourth release corrects one taxi-zone label. This addresses the instruction to provide an update for every dataset as well as exercising correction semantics. Each output is a single physical CSV or Parquet file. The manifest records new, duplicate and modified counts and added fields. Different source end dates are preserved; missing overlap remains visible as missing contextual information.

<!-- pagebreak -->

## Incremental processing and identity

The update path reads each release without replacing existing tables. It validates the source schema, applies the existing preparation functions, isolates invalid rows and selects one deterministic valid survivor per key. A null-safe comparison against Silver separates new rows, modified rows and unchanged duplicates. Only new and changed rows are merged. Records absent from a release are preserved; absence never means deletion.

Weather and air use their existing observation identities, and zones use location ID. These stable keys permit corrections with MERGE. Taxi IDs originally hash timestamps, locations, fare and distance. That hash alone cannot identify a corrected fare. An explicit original_trip_id field resolves this ambiguity: it must reference an existing taxi record and becomes the correction's target ID. Producers must supply it for taxi corrections. No approximate matching is attempted.

When multiple valid versions of a key occur within one release, the Week 1 lexicographic payload rule selects one. This is deterministic, but does not determine the newest real-world event. Releases must arrive in order and should contain one authoritative version per key. A later enhancement would introduce producer sequence numbers, source event IDs and a stale-update check.

## Controlled schema evolution

The additional fields are explicitly configured in config/week3.yaml. Approved nullable numeric columns are cast and validated before MERGE schema evolution. Existing rows receive null, and a later older-format release preserves already stored values for fields it omits. Original source columns must still exist. Unexpected columns, removed columns and Parquet type changes cause the entire readable batch to be quarantined while other datasets continue.

CSV has no reliable physical type declaration, so safe casts and value predicates enforce types at record level. Invalid timestamps, numeric values and required fields are rejected. Structural changes that affect meaning, units, keys or existing analytical columns require manual review and a migration. Automatic acceptance of arbitrary additions would hide producer mistakes.

The weather source already contains rhum. New humidity is kept separately so schema evolution does not silently replace the established relative_humidity_pct meaning used by integration. AQI is likewise stored for future use without rewriting the Week 2 PM2.5 analysis. This preserves compatibility while making new attributes available.

<!-- pagebreak -->

## Analytical consistency and refresh

Changed taxi IDs identify directly affected trips. Changed zone IDs identify trips with either endpoint in those zones. Changed weather or air hours identify trips with a pickup or dropoff in those hours. The pipeline enriches these trips using all current contextual sources and merges them into the existing integrated table. It verifies that enrichment preserves unique trip identities.

The refresh records both old and new pickup dates, which is necessary when a taxi correction moves a trip between days or months. Daily mobility replaces complete affected date groups. Taxi-zone statistics replaces complete affected month groups. Recomputing complete groups avoids incorrect addition of averages and correctly removes obsolete aggregate rows when labels or dates change.

Weather impact contains exposure counts shared across zones, so changing the observed period or weather can affect many groups. The simple implementation fully rebuilds this product when a relevant dataset changes. Air impact contains a continuous hourly calendar, including hours without trips; it is also rebuilt when trips or citywide context change. Both could be maintained more incrementally with explicit calendar and sufficient-statistic state, but that extra complexity is not justified for the small summaries.

A dependency map skips irrelevant products. Weather and air updates do not rebuild fare and distance summaries, and an identical replay skips all product writes. Context updates that match no taxi trips do not rebuild existing products. The daily and monthly SQL is filtered to the relevant groups; a full-table scan may still occur depending on physical pruning. Selective refresh reduces rewritten output and aggregation scope, not necessarily every input read.

## Failures and recovery

Delta commits are atomic per table. Readers can continue reading a valid snapshot during each write, but the release is not a transaction across Silver, integration and four Gold products. The orchestration writes a durable local pending marker before mutations. A failure leaves it present. On the next successful invocation the pipeline repairs integration and all Gold products from current Silver, then clears the marker.

Normal operation therefore uses selective refresh, while recovery deliberately prioritizes correctness through recomputation. The supported deployment has one writer and local paths. There is no distributed lock, exactly-once audit guarantee or automatic rollback of an entire release. Consumers requiring a consistent multi-table view should wait for pipeline success. A larger deployment should publish a release manifest containing committed table versions.

<!-- pagebreak -->

## Validation and monitoring

Generic validation covers source structure, safe casts, missing keys, configured SQL predicates and duplicate keys. Dataset-specific rules cover taxi durations and monetary values, meteorological ranges, sensor completeness and borough mapping. Taxi pickup and dropoff locations are checked against current zone references. Missing weather or air observations remain permitted context gaps, consistent with Week 1; they are not treated as missing mandatory foreign keys.

Invalid rows are excluded before Silver MERGE and therefore do not enter analytical products. Quarantine stores a JSON envelope, a reason, execution ID and timestamp. The envelope accepts heterogeneous malformed schemas without corrupting the quarantine table's schema. Unsupported readable batches are isolated wholesale; an unreadable file is an execution failure recorded in the operational audit.

New ordinary value rules are SQL predicates in configuration. This allows extensions without modifying the core validator. Transformations and complex reference relationships still require dataset-specific code. Adding entirely new datasets also requires extending the Week 1 registry and configuration validator, which currently enumerate four datasets, plus defining analytical dependencies.

Execution metadata includes elapsed time, processed, inserted, updated, rejected and duplicate records, scope exclusions, schema version, validation failures and error messages. Monitoring SQL reports failure frequency and rate, processing duration by dataset, rejections per run and duration trends. Failure rates distinguish repeated problems from datasets merely run more often. Counts help reconcile input handling; duration trends and errors help locate regressions.

Additional production metrics would include source freshness, expected-versus-received volume, schema fingerprints, input checksums, shuffle/spill metrics, executor failures, storage utilization and alert thresholds. Operational history and impact tables require a retention policy. Automatic destructive cleanup is deliberately absent from this course implementation.

## Evaluation and maintainability

The evaluation uses separate copies of the same baseline for normal processing, processing without additional row validation, and processing without operational monitoring. It records update time, integration time, Gold refresh time and physical storage increase. Case order rotates across repetitions; raw samples are retained. Product equality checks guard against an ablation changing semantics. The no-validation case retains casting, schema checks, key checks and duplicate selection, so its result measures the additional rules and reference-checking cost rather than eliminating all validation.

Week 1's reusable prepare and classify steps made maintenance easier. Replacing snapshot publication and introducing correction identity and analytical refresh required the largest changes. The design favors explicit rules and small modules; it avoids a generic dependency engine. For a redesign, stable source trip IDs and versioned releases would be the first changes. At larger scale, partition-aware merges, compacted files, bounded impact retention and managed orchestration would become useful. Performance conclusions belong to the accompanying measured evaluation, not assumptions about production-scale speed.

Reference: Delta Lake documentation, Table deletes updates and merges, https://docs.delta.io/delta-update/.
