"""Focused tests for Week 3 Task 4 reusable validation extensions."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pyspark.sql import DataFrame

from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.base_ingestion import BaseDatasetIngestor
from src.ingestion.validation_rules import validate_schema_compatibility


class FixtureIngestor(BaseDatasetIngestor):
    """Minimal identity ingestor used only by isolated validation tests."""

    def __init__(self, spark, spec: dict) -> None:
        super().__init__(
            spark=spark,
            dataset_name="ValidationFixture",
            dataset_spec=spec,
            schema_version="test-1",
            metadata_path=spec["metadata_path"],
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        return df


class ValidationFrameworkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_spark_session(
            app_name="Week3Task4Validation", master="local[2]", shuffle_partitions=2
        )
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tempdir.name)
        cls.reference_path = str(cls.root / "zones")
        cls.spark.createDataFrame([(1,), (2,)], ["location_id"]).write.format(
            "delta"
        ).save(cls.reference_path)

    @classmethod
    def tearDownClass(cls) -> None:
        stop_spark_session(cls.spark)
        cls.tempdir.cleanup()

    def spec(self, *, reference_checks: list[dict[str, str]] | None = None) -> dict:
        return {
            "source_path": "unused",
            "bronze_path": None,
            "silver_path": str(self.root / "silver"),
            "quarantine_path": str(self.root / "quarantine"),
            "metadata_path": str(self.root / "metadata"),
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

    def classified(self, rows, *, reference_checks=None):
        frame = self.spark.createDataFrame(
            rows, ["id", "required_text", "zone_id", "amount"]
        )
        ingestor = FixtureIngestor(
            self.spark, self.spec(reference_checks=reference_checks)
        )
        return ingestor, ingestor.apply_data_quality_rules(frame)

    def test_required_values_reject_null_and_blank_strings(self) -> None:
        _, (valid, rejected) = self.classified(
            [("valid", "present", 1, 1.0), ("null", None, 1, 1.0), ("blank", " ", 1, 1.0)]
        )
        self.assertEqual(valid.count(), 1)
        reasons = {row.id: row._rejection_reason for row in rejected.select("id", "_rejection_reason").collect()}
        self.assertEqual(reasons["null"], "missing_value:required_text")
        self.assertEqual(reasons["blank"], "missing_value:required_text")

    def test_reference_integrity_rejects_only_missing_non_null_keys(self) -> None:
        checks = [
            {
                "source_field": "zone_id",
                "reference_path": self.reference_path,
                "reference_field": "location_id",
                "rule_name": "missing_reference:zone_id",
            }
        ]
        _, (valid, rejected) = self.classified(
            [("valid", "present", 1, 1.0), ("invalid", "present", 99, 1.0)],
            reference_checks=checks,
        )
        self.assertEqual(valid.select("id").first()[0], "valid")
        self.assertEqual(
            rejected.select("_rejection_reason").first()[0], "missing_reference:zone_id"
        )

    def test_schema_compatibility_accepts_declared_additions(self) -> None:
        result = validate_schema_compatibility(
            ["id", "amount", "humidity"],
            {
                "expected_canonical_fields": ["id", "amount"],
                "supported_additive_fields": ["humidity"],
            },
        )
        self.assertTrue(result.is_compatible)
        self.assertEqual(result.supported_additive_fields_seen, ["humidity"])

    def test_schema_compatibility_reports_missing_and_unexpected_fields(self) -> None:
        missing = validate_schema_compatibility(
            ["id"],
            {
                "expected_canonical_fields": ["id", "amount"],
                "supported_additive_fields": [],
            },
        )
        unexpected = validate_schema_compatibility(
            ["id", "amount", "surprise"],
            {
                "expected_canonical_fields": ["id", "amount"],
                "supported_additive_fields": [],
            },
        )
        self.assertFalse(missing.is_compatible)
        self.assertEqual(missing.missing_required_fields, ["amount"])
        self.assertFalse(unexpected.is_compatible)
        self.assertEqual(unexpected.unexpected_fields, ["surprise"])

    def test_new_rules_coexist_with_existing_sql_and_cast_failures(self) -> None:
        spec = self.spec()
        spec["casts"] = {"amount": "double"}
        ingestor = FixtureIngestor(self.spark, spec)
        raw = self.spark.createDataFrame(
            [("bad", "", "1", "not-a-number")],
            ["id", "required_text", "zone_id", "amount"],
        )
        _, rejected = ingestor.apply_data_quality_rules(
            ingestor.standardize_column_names(raw)
        )
        errors = rejected.select("_validation_errors").first()[0]
        self.assertIn("invalid_type:amount", errors)
        self.assertIn("valid_amount", errors)
        self.assertIn("missing_value:required_text", errors)

    def test_duplicate_behavior_remains_deterministic(self) -> None:
        _, (valid, rejected) = self.classified(
            [("same", "present", 1, 2.0), ("same", "present", 1, 1.0)]
        )
        self.assertEqual(valid.count(), 1)
        self.assertEqual(valid.select("amount").first()[0], 1.0)
        self.assertEqual(rejected.select("_rejection_reason").first()[0], "duplicate_key")

    def test_rejected_rows_can_use_shared_quarantine_writer(self) -> None:
        ingestor, (_, rejected) = self.classified(
            [("bad", "", 1, 1.0)]
        )
        ingestor.persist_rejected_records(rejected, run_id="known-run")
        persisted = self.spark.read.format("delta").load(ingestor.quarantine_path)
        row = persisted.select(
            "_rejection_reason", "_run_id", "_schema_version", "_rejected_at"
        ).first()
        self.assertEqual(row._rejection_reason, "missing_value:required_text")
        self.assertEqual(row._run_id, "known-run")
        self.assertEqual(row._schema_version, "test-1")
        self.assertIsNotNone(row._rejected_at)


if __name__ == "__main__":
    unittest.main(verbosity=2)
