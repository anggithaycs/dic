"""Small reusable helpers for declarative ingestion validation rules."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass(frozen=True)
class SchemaCompatibilityResult:
    """Result of comparing a canonical batch schema with its declared contract."""

    missing_required_fields: list[str]
    unexpected_fields: list[str]
    supported_additive_fields_seen: list[str]
    is_compatible: bool

    def as_dict(self) -> dict[str, Any]:
        """Return a serializable representation suitable for callers and evidence."""
        return asdict(self)


def required_value_error_columns(
    df: DataFrame, required_values: Iterable[str]
) -> list[Column]:
    """Build per-field missing-value error expressions for an input DataFrame."""
    string_fields = {
        field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
    }
    errors: list[Column] = []
    for field in required_values:
        if field not in df.columns:
            raise ValueError(f"Required-value rule references missing field: {field}")
        missing = F.col(field).isNull()
        if field in string_fields:
            missing = missing | (F.trim(F.col(field)) == "")
        errors.append(F.when(missing, F.lit(f"missing_value:{field}")))
    return errors


def apply_reference_checks(
    spark: SparkSession, df: DataFrame, reference_checks: Iterable[dict[str, str]]
) -> DataFrame:
    """Add ``_reference_errors`` for non-null values absent from Delta references."""
    result = df.withColumn("_reference_errors", F.array().cast("array<string>"))
    for index, check in enumerate(reference_checks):
        source_field = check["source_field"]
        reference_path = check["reference_path"]
        reference_field = check["reference_field"]
        rule_name = check["rule_name"]
        if source_field not in result.columns:
            raise ValueError(
                f"Reference rule '{rule_name}' references missing field: {source_field}"
            )

        lookup_value = f"_validation_reference_value_{index}"
        lookup_match = f"_validation_reference_match_{index}"
        reference_df = (
            spark.read.format("delta")
            .load(reference_path)
            .select(F.col(reference_field).alias(lookup_value))
            .where(F.col(lookup_value).isNotNull())
            .distinct()
            .withColumn(lookup_match, F.lit(True))
        )
        result = result.join(
            F.broadcast(reference_df),
            F.col(source_field) == F.col(lookup_value),
            "left",
        )
        failure = F.when(
            F.col(source_field).isNotNull() & F.col(lookup_match).isNull(),
            F.lit(rule_name),
        )
        result = (
            result.withColumn(
                "_reference_errors",
                F.concat(
                    F.col("_reference_errors"),
                    F.filter(F.array(failure), lambda value: value.isNotNull()),
                ),
            )
            .drop(lookup_value, lookup_match)
        )
    return result


def validate_schema_compatibility(
    actual_fields: Iterable[str], schema_contract: dict[str, Any]
) -> SchemaCompatibilityResult:
    """Compare fields to a declared canonical schema and optional additive fields."""
    actual = set(actual_fields)
    expected = set(schema_contract.get("expected_canonical_fields", []))
    supported_additive = set(schema_contract.get("supported_additive_fields", []))
    missing = sorted(expected - actual)
    additive_seen = sorted(actual & supported_additive)
    unexpected = sorted(actual - expected - supported_additive)
    return SchemaCompatibilityResult(
        missing_required_fields=missing,
        unexpected_fields=unexpected,
        supported_additive_fields_seen=additive_seen,
        is_compatible=not missing and not unexpected,
    )
