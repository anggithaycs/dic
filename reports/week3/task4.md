# Task 4 — Extended Data Validation Framework

## 1. Objective

Task 4 extends the Week 1 validation framework to detect duplicate records, invalid attribute values, missing reference records, unsupported schema changes, and incomplete records.

Invalid row-level records do not interrupt processing: they are isolated, reported with diagnostics, and excluded from analytical data. Schema incompatibility is treated as a batch-level failure, rather than fabricating an invalid row for every input record.

## 2. Validation Architecture

```text
Input batch
  -> schema compatibility check
  -> preparation and safe casting
  -> generic and dataset-specific validation rules
  -> reference-integrity checks
  -> duplicate classification
  -> accepted versus rejected split
  -> rejected rows to quarantine
  -> accepted rows to Silver and downstream analytics
```

The implementation extends the existing ingestion framework rather than introducing another validation pipeline. [`validation_rules.py`](../../src/ingestion/validation_rules.py) provides reusable required-value, reference-integrity, and schema-compatibility helpers. [`BaseDatasetIngestor`](../../src/ingestion/base_ingestion.py) remains the central classifier and provides shared quarantine persistence. Rules are declared in [`platform_config.yaml`](../../config/platform_config.yaml).

## 3. Generic Validation Rules

The reusable mechanisms are:

- Required-value checks, including blank strings where applicable.
- Missing business-key checks.
- Safe-cast/type failures, reported as `invalid_type:<field>`.
- Deterministic duplicate business-key classification.
- Batch-level schema compatibility checks.
- Declarative Delta reference-integrity checks.
- Named SQL predicates defined in configuration.

The mechanisms are generic; rule content remains dataset-specific. For example, the framework evaluates named SQL predicates consistently, while Taxi fare ranges and Weather measurement bounds remain dataset contracts.

## 4. Dataset-Specific Validation Rules

Taxi requires pickup and dropoff locations, validates both against the Taxi Zone reference table, and retains existing trip-time, duration, fare, distance, passenger, and location rules.

Weather requires core calendar and measurement fields, retains its configured meteorological bounds, and allows the known additive `humidity` field.

Air Quality requires site identifiers and the PM2.5 measurement, retains configured PM2.5 rules, and allows the known additive `aqi` field.

Taxi Zones require lookup identifiers and descriptive fields. Thresholds and business-specific predicates remain configuration-driven where possible.

## 5. Extensibility

New SQL attribute rules generally require only a configuration entry. Required fields are added through `required_values`; reference checks through `reference_checks`; and allowlisted schema additions through `schema_compatibility`.

New reusable rule families belong in [`validation_rules.py`](../../src/ingestion/validation_rules.py), without rewriting the overall ingestion lifecycle in [`BaseDatasetIngestor`](../../src/ingestion/base_ingestion.py). A dataset-specific ingestor is still appropriate when a new rule depends on deriving a new field before validation.

## 6. Invalid-Record Handling

Validation errors accumulate in `_validation_errors`; `_rejection_reason` records the final reason. Rejected rows retain those diagnostics and receive `_run_id`, `_schema_version`, and `_rejected_at` when written to quarantine through the shared `persist_rejected_records` method.

Accepted rows exclude validation diagnostics. Rejected rows are excluded from Silver and therefore from downstream integration and Gold products. The Week 3 incremental scripts now use the shared persistence method after their existing rejection classification, without changing their Delta `MERGE` predicates.

## 7. Validation Demonstration Results

The controlled temporary-Delta demonstration is recorded in [`task4_validation.json`](../../storage/metrics/week3/task4_validation.json).

### Duplicate

- Input: 2
- Accepted: 1
- Rejected: 1
- Reason: `duplicate_key`

### Invalid Attribute

- Input: 1
- Accepted: 0
- Rejected: 1
- Reason: `valid_amount`

### Missing Reference

- Input: 1
- Accepted: 0
- Rejected: 1
- Reason: `missing_reference:zone_id`

### Incomplete Record

- Input: 1
- Accepted: 0
- Rejected: 1
- Reason: `missing_value:required_text`

### Unsupported Schema

The batch was incompatible because of `unexpected_field`. No row-level rejected count was fabricated. The temporary published table contained 1 row before and after the rejected publication attempt.

### Supported Schema Evolution

`humidity` was accepted: `is_compatible = true` and `supported_additive_fields_seen = ["humidity"]`.

## 8. Why Row-Level Errors Do Not Interrupt Processing

Row-level failures are represented as validation errors, then split away from accepted records. Valid rows continue through processing while invalid rows are quarantined. Only an incompatible batch-level schema stops publication of that batch.

The demonstration confirms this accounting for every row-level case: input records equal accepted plus rejected records, and each rejected case was isolated and excluded from accepted output.

## 9. Analytical Data Protection

The framework protects analytical outputs by excluding rejected rows from Silver. The incremental scripts also exclude them from their MERGE sources, so they cannot enter the incremental targets. Downstream integration and Gold products consume accepted data only.

This behavior was demonstrated with isolated temporary Delta paths. Production pipelines were not rerun for Task 4.

## 10. Schema Validation Design

Row-level type conversion uses the existing safe-cast behavior and produces `invalid_type:<field>` errors.

Batch-level schema compatibility separately reports `missing_required_fields`, `unexpected_fields`, `supported_additive_fields_seen`, and `is_compatible`. Missing required fields and unallowlisted additions prevent publication; explicitly supported additions are accepted. The current contract is field-level, not a complete physical-type schema contract.

## 11. Testing and Evidence

[`test_week3_task4_validation.py`](../../tests/test_week3_task4_validation.py) passed all **7 of 7** Task 4 validation tests. Syntax compilation and `git diff --check` also passed.

The controlled demonstration used temporary Delta paths only. Its integrity checks show that production Silver/Gold Delta versions remained unchanged and that existing Task 1, Task 2, and Task 3 evidence hashes were unchanged.

## 12. Limitations

- Production incremental scripts were not rerun after quarantine persistence was added; integration was validated through the shared helper, unit tests, and the temporary-path demonstration.
- Schema compatibility is field-level.
- Batch-level schema failures are surfaced to callers but are not persisted as a dedicated batch-failure audit artifact.
- Per-rule aggregate validation statistics could be added in a future extension.

## 13. Conclusion

Task 4 extends the Week 1 validation framework with reusable required-value, reference-integrity, and schema-compatibility validation while preserving existing duplicate and attribute-validation behavior and the quarantine flow.
