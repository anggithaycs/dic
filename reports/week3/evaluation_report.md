# Week 3 evaluation
## Full-data update and replay results

The supplied-source release was successfully applied to the main pipeline. Run 8b846d942ea84336a7e8a89a2c70116d processed 859,931 taxi rows: 716,609 inserted and 143,322 duplicates. It also inserted 48 weather and 288 air observations and updated one zone. All four datasets reported zero rejected records and zero validation failures.

The update stage took 125.85 seconds, integration 503.34 seconds and analytical refresh 45.70 seconds. Total measured time was 674.90 seconds (11 minutes 15 seconds), excluding the final pipeline audit write. Integration covered 739,033 affected trips and dominated elapsed time. All four analytical products were refreshed: affected date/month groups for mobility and zone statistics, and full weather/air summaries.

There is one recorded full-data update, so its timings are observations rather than repeated-run medians. No before/after storage measurement is recorded for that initial update. Evidence is preserved in evidence/full_update.json.

## Repeated full-data replay benchmark

The nine evaluation samples used independent copies of an already-updated baseline, with three repetitions per case. Every run processed 859,931 taxi rows as duplicates, made no inserts or updates across all datasets and reported zero affected trips. These measurements therefore describe replay detection and audit overhead; they do not measure first-application merge or analytical recomputation costs. Every sample passed the product-equivalence check.

| Replay case | Update s | Gold refresh s | Total s | Growth KiB |
|---|---:|---:|---:|---:|
| Full processing | 163.37 | 1.93 | 179.10 | 82.6 |
| No extra validation | 157.62 | 3.47 | 176.08 | 82.6 |
| No monitoring | 160.78 | 3.69 | 178.75 | 24.4 |

Median total-time differences were 3.02 seconds for extra validation and 0.35 seconds for monitoring. These small differences are noisy comparisons, not isolated feature costs. Growth includes retained Delta files, audit output and run JSON; it is not initial-update storage growth. Raw samples are in evidence/full_replay_measurements.json. Runtime settings were not recorded in these JSON results, so the fixture settings below must not be assumed for this run.

<!-- pagebreak -->

## Full-data interpretation and limitations

Integration accounted for 74.6% of the initial update time. The implementation limits enrichment to affected trips, but identifying those trips and merging by trip ID still accesses the existing large tables. A zone correction can affect historical trips. The current merge has no explicit partition restriction, so a small release does not imply proportionally small read or rewrite work. These are code-based explanations; the saved phase timings do not isolate individual scans, joins or file rewrites.

The full-data run demonstrates successful processing of this release, including duplicate detection, schema additions and analytical refresh. The nine replays demonstrate repeatability after the release is present. Their product-equivalence checks compare replay cases with each other; they do not establish equivalence of the initial full-data refresh to an independent full recomputation. Full-data failure injection and recovery were not measured.

## Supporting fixture correctness checks

A separate 200-trip fixture provides targeted checks that are easier to inspect than the full dataset. Its release contains 15 new taxi trips, 3 duplicates, 4 weather rows, 4 air rows and one zone correction. The acceptance run inserted the expected rows, preserved old rows with nulls in added fields, and replayed without inserts or updates. All four products were skipped on replay.

The fixture also verified invalid-zone rejection, unsupported-schema rejection, an explicit fare correction, pending-refresh recovery and all six legacy query answers. Selective refresh matched full recomputation for all four products. These are fixture-level correctness results, not additional full-data performance measurements. Evidence is retained in evidence/acceptance.json.

Nine separate fixture ablations (three per case) had median total times of 81.60 seconds for normal processing, 70.74 seconds without extra validation and 67.92 seconds without monitoring. The fixture used local[2], two shuffle partitions and a 2 GiB driver with Spark 4.0.1, Delta 4.0.1, Python 3.12 and Java 21 on Windows. These small-workload timings are supporting evidence only; raw samples and their summary remain in evidence/measurements.json and evidence/summary.json.

## Maintainability and scalability

The solution reuses Week 1 normalization and validation and Week 2 analytical SQL. Changed rows are staged before MERGE so target-table updates cannot invalidate the comparison used to identify changes. Audit tables retain dataset counts, timings and failures. The implementation assumes one writer; readers should wait for pipeline completion because publication across several tables is not atomic.

The measured priority for improvement is integration. Partition-aware merge predicates and reuse of affected-trip IDs are candidates for reducing repeated reads, but require correctness checks and representative benchmarks. Retention policies and file compaction would control accumulated history and small files. Stable source trip IDs would simplify corrections. Extending the registry with a dataset requires its schema, validation, keys and refresh dependencies.

The available evidence supports a working local batch solution at the supplied data volume. It does not establish distributed scalability or the overhead of validation and monitoring during a first full-data update. Those claims would require repeated runs from the same pre-update baseline, with saved runtime settings and before/after storage measurements.
