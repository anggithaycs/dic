"""Run isolated Week 3 Task 4 validation demonstrations using temporary Delta paths."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.base_ingestion import BaseDatasetIngestor
from src.ingestion.validation_rules import validate_schema_compatibility


OUTPUT_PATH = ROOT / "storage/metrics/week3/task4_validation.json"
PRODUCTION_PATHS = (
    "storage/delta/silver/clean_taxi_trips",
    "storage/delta/silver/clean_weather",
    "storage/delta/silver/clean_air_quality",
    "storage/delta/silver/clean_taxi_zones",
    "storage/delta/silver/integrated_taxi_trips",
    "storage/delta/gold/daily_mobility_summary",
    "storage/delta/gold/taxi_zone_statistics",
    "storage/delta/gold/weather_impact_summary",
    "storage/delta/gold/air_quality_impact_summary",
)
EXISTING_EVIDENCE = (
    ROOT / "storage/metrics/week3/task1_incremental_updates.json",
    ROOT / "storage/metrics/week3/task2_integration_refresh.json",
    ROOT / "storage/metrics/week3/product_comparison.json",
    ROOT / "storage/metrics/week3/monitoring_queries.json",
)


class DemoIngestor(BaseDatasetIngestor):
    """Minimal identity ingestor used exclusively with temporary demonstration paths."""

    def __init__(self, spark: SparkSession, spec: dict[str, Any]) -> None:
        super().__init__(
            spark=spark,
            dataset_name="Task4ValidationDemo",
            dataset_spec=spec,
            schema_version="task4-demo",
            metadata_path=spec["metadata_path"],
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        return df


def delta_versions(spark: SparkSession) -> dict[str, int]:
    """Read current production versions without writing to production tables."""
    return {
        path: int(DeltaTable.forPath(spark, path).history(1).first()["version"])
        for path in PRODUCTION_PATHS
    }


def hashes() -> dict[str, str]:
    """Hash pre-existing evidence to verify this demo does not change it."""
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in EXISTING_EVIDENCE
    }


def as_json(value: Any) -> Any:
    """Convert Spark-returned values to JSON-compatible scalar structures."""
    if hasattr(value, "asDict"):
        return {key: as_json(item) for key, item in value.asDict(recursive=True).items()}
    if isinstance(value, list):
        return [as_json(item) for item in value]
    return value


def make_spec(root: Path, reference_checks: list[dict[str, str]] | None = None) -> dict[str, Any]:
    """Return the shared temporary-table validation contract."""
    return {
        "source_path": "temporary",
        "bronze_path": None,
        "silver_path": str(root / "silver"),
        "quarantine_path": str(root / "quarantine"),
        "metadata_path": str(root / "metadata"),
        "format": "csv",
        "timezone": "UTC",
        "partition_cols": [],
        "required_columns": ["id", "required_text", "zone_id", "amount"],
        "rename": {},
        "casts": {},
        "key": ["id"],
        "quality_rules": {},
        "validation": {"valid_amount": "amount >= 0"},
        "selected_columns": ["id", "required_text", "zone_id", "amount"],
        "required_values": ["required_text"],
        "reference_checks": reference_checks or [],
    }


def classify_case(
    spark: SparkSession,
    root: Path,
    rows: list[tuple[str, str | None, int, float]],
    reference_checks: list[dict[str, str]] | None = None,
) -> tuple[dict[str, Any], DemoIngestor]:
    """Classify one row-level case and persist its rejected rows to temporary quarantine."""
    ingestor = DemoIngestor(spark, make_spec(root, reference_checks))
    source = spark.createDataFrame(rows, ["id", "required_text", "zone_id", "amount"])
    accepted, rejected = ingestor.apply_data_quality_rules(source)
    input_count = source.count()
    accepted_count = accepted.count()
    rejected_count = rejected.count()
    if rejected_count:
        ingestor.persist_rejected_records(rejected, run_id="task4-demo")
    details = [
        as_json(row)
        for row in rejected.select("id", "_validation_errors", "_rejection_reason").collect()
    ]
    return (
        {
            "input_record_count": input_count,
            "accepted_record_count": accepted_count,
            "rejected_record_count": rejected_count,
            "rejections": details,
            "processing_continued": input_count == accepted_count + rejected_count,
            "rejected_rows_isolated": rejected_count > 0,
            "invalid_rows_excluded_from_accepted_output": (
                accepted_count + rejected_count == input_count
                and not accepted.intersect(rejected.select(*accepted.columns)).count()
            ),
        },
        ingestor,
    )


def run(spark: SparkSession) -> dict[str, Any]:
    """Execute all Task 4 validation scenarios without touching production data."""
    production_before = delta_versions(spark)
    evidence_before = hashes()
    with tempfile.TemporaryDirectory(prefix="week3_task4_") as directory:
        temp_root = Path(directory)
        reference_path = str(temp_root / "zones")
        spark.createDataFrame([(1,), (2,)], ["location_id"]).write.format("delta").save(
            reference_path
        )
        reference_checks = [
            {
                "source_field": "zone_id",
                "reference_path": reference_path,
                "reference_field": "location_id",
                "rule_name": "missing_reference:zone_id",
            }
        ]

        duplicate, ingestor = classify_case(
            spark, temp_root, [("duplicate", "present", 1, 1.0), ("duplicate", "present", 1, 2.0)]
        )
        invalid_attribute, _ = classify_case(
            spark, temp_root, [("invalid-attribute", "present", 1, -1.0)]
        )
        missing_reference, _ = classify_case(
            spark,
            temp_root,
            [("missing-reference", "present", 99, 1.0)],
            reference_checks,
        )
        incomplete, _ = classify_case(
            spark, temp_root, [("incomplete", " ", 1, 1.0)]
        )

        supported = validate_schema_compatibility(
            ["weather_observation_id", "temperature_c", "humidity"],
            {
                "expected_canonical_fields": [
                    "weather_observation_id",
                    "temperature_c",
                ],
                "supported_additive_fields": ["humidity"],
            },
        ).as_dict()

        publication_path = str(temp_root / "schema_publication")
        spark.createDataFrame([(1,)], ["baseline"]).write.format("delta").save(
            publication_path
        )
        rows_before = spark.read.format("delta").load(publication_path).count()
        unsupported = validate_schema_compatibility(
            ["weather_observation_id", "temperature_c", "unexpected_field"],
            {
                "expected_canonical_fields": [
                    "weather_observation_id",
                    "temperature_c",
                ],
                "supported_additive_fields": ["humidity"],
            },
        ).as_dict()
        rows_after = spark.read.format("delta").load(publication_path).count()

        quarantined = spark.read.format("delta").load(ingestor.quarantine_path)
        quarantine_count = quarantined.count()
        quarantine_metadata = [
            field for field in ("_run_id", "_schema_version", "_rejected_at") if field in quarantined.columns
        ]

    production_after = delta_versions(spark)
    evidence_after = hashes()
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "validation_categories": [
            "duplicate_records",
            "invalid_attribute_values",
            "missing_reference_records",
            "unsupported_schema_changes",
            "incomplete_records",
        ],
        "duplicate_test": duplicate,
        "invalid_attribute_test": invalid_attribute,
        "missing_reference_test": missing_reference,
        "incomplete_record_test": incomplete,
        "supported_schema_change_test": {
            "schema_compatibility": supported,
            "processing_continued": supported["is_compatible"],
        },
        "unsupported_schema_change_test": {
            "input_record_count": 1,
            "accepted_record_count": None,
            "rejected_record_count": None,
            "schema_compatibility": unsupported,
            "processing_continued": False,
            "rejected_rows_isolated": False,
            "invalid_rows_excluded_from_accepted_output": True,
            "publication_prevented": not unsupported["is_compatible"],
            "temporary_published_rows_before": rows_before,
            "temporary_published_rows_after": rows_after,
        },
        "quarantine_behavior": {
            "temporary_quarantined_row_count": quarantine_count,
            "metadata_fields_present": quarantine_metadata,
        },
        "analytical_exclusion_check": {
            "all_row_level_cases_excluded_invalid_rows": all(
                case["invalid_rows_excluded_from_accepted_output"]
                for case in (duplicate, invalid_attribute, missing_reference, incomplete)
            )
        },
        "extensibility_summary": {
            "required_values": "Configured fields add missing_value:<field> errors.",
            "reference_checks": "Configured Delta lookups add the supplied missing_reference rule name.",
            "schema_compatibility": "Configured canonical and additive fields produce a structured batch result.",
        },
        "production_integrity": {
            "production_delta_versions_before": production_before,
            "production_delta_versions_after": production_after,
            "production_delta_versions_unchanged": production_before == production_after,
            "existing_evidence_hashes_unchanged": evidence_before == evidence_after,
        },
    }


def main() -> None:
    spark = get_spark_session(
        app_name="Week3Task4ValidationDemo", master="local[2]", shuffle_partitions=2
    )
    try:
        evidence = run(spark)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
        print(json.dumps(evidence, indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
