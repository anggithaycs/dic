# Week 3

This part extends the Week 1 and 2 pipeline to handle new data, duplicates and corrections, then refresh the affected analytical tables. It also adds validation for the new columns and records processing metrics.

## Code

The implementation is in [src/week3](../../src/week3/). The [README there](../../src/week3/README.md) has the setup and run commands.

- [generate.py](../../src/week3/generate.py) creates the update files for taxis, weather, air quality and zones.
- [updates.py](../../src/week3/updates.py) validates incoming rows, handles duplicates and merges changes into the existing Delta tables. Invalid rows go to quarantine.
- [refresh.py](../../src/week3/refresh.py) updates the affected integrated trips and analytical tables.
- [run.py](../../src/week3/run.py) is the command-line entry point for generation, updates, monitoring and evaluation.
- [monitoring.sql](../../src/week3/monitoring.sql) contains the five queries for checking counts, failures and processing times.
- [evaluate.py](../../src/week3/evaluate.py) compares normal processing with validation or monitoring disabled.
- [config/week3.yaml](../../config/week3.yaml) contains the Week 3 validation rules and allowed schema additions.

## Reports and tests

- [Design report](design_report.md): how the update, validation, refresh and monitoring steps work.
- [Evaluation report](evaluation_report.md): results from nine full-data update experiments, with the smaller tests used to check correctness.
- [test_week3.py](../../tests/test_week3.py): the 200-trip test dataset and checks for duplicates, schema changes, invalid rows, corrections, recovery and query results.
- [plan.md](plan.md): the implementation plan.

## Data and results

The generated update files are in `data/week3_release/`. They contain 716,609 new taxi trips, 143,322 duplicate trips, 48 weather rows, 288 air-quality rows and one zone correction. Weather adds humidity and air quality adds AQI. The data files are excluded from Git and can be recreated with the generator.

The latest evaluation is in `storage/metrics/week3/b4ea2164845846b4b4fc9825c2e218cc/`. All nine runs started from copies of the pre-update baseline and inserted the new records. Normal processing took a median of 673.69 seconds, about 11 minutes 14 seconds. The report compares it with processing without extra validation and without monitoring, with three repetitions per configuration.

Copies of the results used in the report are in [evidence/](evidence/):

- [full_evaluation_measurements.json](evidence/full_evaluation_measurements.json) and [full_evaluation_summary.json](evidence/full_evaluation_summary.json): the latest nine full-data update experiments used in the report.
- [acceptance.json](evidence/acceptance.json): results from the 200-trip correctness tests.
- [full_release_manifest.json](evidence/full_release_manifest.json) and [full_release_verification.json](evidence/full_release_verification.json): generated file counts and validation results.
