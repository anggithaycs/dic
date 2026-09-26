# Week 3 evaluation

## Workload and method

The evaluation used the full-data Silver and Gold tables rebuilt from Weeks 1 and 2, before applying the Week 3 release. Each experiment started from its own copy of this baseline. This means every run processed new records and corrections, rather than replaying an update that was already present.

Three configurations were tested: normal processing, processing without the additional row-validation rules, and processing without operational monitoring. Each configuration ran three times, giving nine samples. The execution order rotated between repetitions. Spark's cache was cleared between cases, but the same Spark session and filesystem were reused, so this was not a cold-start benchmark.

The release contained 859,931 taxi rows, 48 weather observations, 288 air observations and one zone correction. Every run inserted 716,609 taxi trips, recognised 143,322 existing taxi duplicates, inserted the weather and air rows, and updated the zone. Humidity and AQI were introduced as nullable columns. All nine runs completed successfully.

The figures below are calculated from the saved measurements in `storage/metrics/week3/b4ea2164845846b4b4fc9825c2e218cc/`. Copies are included in [evidence/full_evaluation_measurements.json](evidence/full_evaluation_measurements.json) and [evidence/full_evaluation_summary.json](evidence/full_evaluation_summary.json). The measurements do not include a runtime-configuration snapshot, so they cannot establish performance under a precisely reproducible hardware and Spark configuration.

## Incremental update and analytical refresh performance

| Configuration | Update (s) | Integration (s) | Gold refresh (s) | Total (s) |
|---|---:|---:|---:|---:|
| Normal processing | 123.27 | 491.88 | 80.02 | 673.69 |
| Without extra validation | 112.52 | 575.31 | 56.02 | 748.99 |
| Without monitoring | 120.30 | 426.97 | 45.22 | 593.46 |

Each value is the median of three runs. Phase medians are calculated separately, so they do not necessarily add up to the median total. The normal run took about 11 minutes 14 seconds, with individual totals ranging from 646.22 to 723.98 seconds. Update timing includes loading, preparation, validation, change detection, merges and dataset audit writes. Total timing excludes baseline copying, source generation, Spark startup, post-run product comparisons and the final pipeline-status write.

Integration was the largest cost, accounting for 64.5% to 76.1% of normal-run time. Each run refreshed 739,033 affected trips. This exceeds the number of inserted trips because reference-data corrections can also affect existing trips. Finding affected records and merging them into the integrated table still requires access to large existing tables, even though only a subset is enriched. The phase measurements identify integration as the bottleneck, but do not separate scan, join and file-rewrite costs.

All four Gold products were refreshed in every run. Daily mobility replaced affected date groups and taxi-zone statistics replaced affected month groups. Weather and air summaries were fully recomputed. Their median individual refresh times in normal processing were 10.33, 8.85, 10.12 and 19.60 seconds respectively. The overall Gold stage also includes shared preparation and audit work; its median cannot be reconstructed by adding the separate product medians.

## Validation and monitoring results

No dataset reported rejected rows or validation failures in any run. In the configurations where validation was enabled, this shows that the generated release passed the checks. Zero failures with validation disabled is not independent evidence of data quality. The normal runs recorded schema version 3.1 for weather and air and 3.0 for taxis and zones.

The normal configuration spent a median of 23.93 seconds in the recorded validation stages, summed across datasets within each run. Disabling additional validation reduced median update time by 10.76 seconds, but increased median total time by 75.30 seconds. The latter is not a negative cost of validation: integration varied substantially between runs, and the no-extra-validation totals ranged from 600.31 to 944.69 seconds. Three repetitions are insufficient to isolate the feature's overall cost from this variation. Schema checks, casting, key checks and duplicate selection remain enabled in that configuration.

Monitoring identified taxis as the slowest dataset to process: its median dataset duration was 82.04 seconds, compared with 8.90 seconds for zones, 11.32 for weather and 12.24 for air. These dataset durations cover their update work, not the later integration stage. Across the recorded samples, execution status was successful and reported validation-failure frequency was zero. Counts and timings therefore make it possible to distinguish the large taxi update from the more expensive shared integration work.

Disabling monitoring reduced median total time by 80.23 seconds, about 11.9% of the normal median. This is an observed comparison rather than a precise audit-writing cost: integration also ran faster in that configuration. Monitoring introduces Delta writes and metadata work, but the experiment does not isolate these from execution variability.

Physical storage increased by a median of 1,371.13 MiB in normal processing, 1,371.14 MiB without extra validation and 1,371.07 MiB without monitoring. This includes new data, retained old Delta files, transaction logs, staged changes and operational records. No cleanup was performed. The normal and no-monitoring median increases differ by only about 65.2 KiB; this difference should not be treated as an exact measurement of monitoring storage. Most measured growth is associated with data processing and retained versions.

## Correctness checks and their limits

Every evaluation sample passed the comparison of all four analytical products against the first normal-processing result. Disabling validation or monitoring therefore did not change the measured product contents for this valid release. This comparison does not independently prove that the full-data incremental results equal a complete rebuild, because the reference was produced by the same incremental implementation.

Separate tests using the 200-trip fixture checked duplicates, nullable schema additions, invalid zone references, unsupported schemas, explicit taxi corrections and recovery after an interrupted refresh. They also checked selective refresh against full recomputation and the six existing query results. These tests support correctness; their timings are not used as the main performance evidence in this report. The full-data evaluation did not inject failures or invalid records, so zero rejections alone cannot demonstrate that invalid inputs would be handled correctly.

The saved fixture results are in [evidence/acceptance.json](evidence/acceptance.json). The release counts and source-file checks are retained in [evidence/full_release_manifest.json](evidence/full_release_manifest.json) and [evidence/full_release_verification.json](evidence/full_release_verification.json).

## Maintainability and scalability

The implementation reuses Week 1 preparation and validation and Week 2 analytical SQL. Generation, updates, refresh and monitoring are kept in separate modules, making it easier to change one part without rewriting the others. Simple value rules can be extended through configuration. New datasets still require their keys, transformations, reference checks and refresh dependencies to be defined explicitly.

The main maintenance complications are taxi correction identity, tracking both old and new analytical groups, and recovering when a run stops between table commits. Staging changed rows before merging preserves the change set used by refresh. A pending marker triggers recomputation after failure. This is manageable for a local batch application, but it assumes one writer and does not make publication across all tables atomic.

The measurements show that the solution handles the supplied workload, but processing time is not proportional only to the number of new rows. Integration remains expensive because affected-row selection and merges can access much larger tables. Partition-aware merge matching and reuse of affected IDs are the first improvements to investigate. Full recomputation of the two context summaries is another possible optimisation, although integration is the larger measured cost.

Repeated runs also retain substantial storage. A longer-running deployment would need retention rules and file compaction, while stable source IDs and ordered releases would simplify corrections. Distributed scalability has not been measured. Before adding more infrastructure, the next evaluation should save the exact runtime settings, use more repetitions under controlled machine load, and measure integration's scan and shuffle costs.
