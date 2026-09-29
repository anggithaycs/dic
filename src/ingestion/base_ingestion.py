"""Generic Extensible Ingestion Engine for Urban Data Platform.

Implements Medallion Architecture:
- Bronze Layer: Ingests raw source data with source fidelity and audit timestamps.
- Silver Layer: Validates schema, standardizes column naming (snake_case), normalizes timestamps,
  and isolates invalid records into quarantine before publishing clean Delta tables.
"""

import json
import re
import time
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.logger import get_logger
from src.ingestion.validation_rules import (
    SchemaCompatibilityResult,
    apply_reference_checks,
    required_value_error_columns,
    validate_schema_compatibility,
)


class BaseDatasetIngestor(ABC):
    """Abstract Generic Ingestion Engine supporting Bronze and Silver tiers.

    Subclasses implement dataset-specific transformation and normalization logic.
    """

    def __init__(
        self,
        spark: SparkSession,
        dataset_name: str,
        dataset_spec: dict[str, Any],
        schema_version: str,
        metadata_path: str,
    ) -> None:
        self.spark = spark
        self.dataset_name = dataset_name
        self.dataset_spec = deepcopy(dataset_spec)
        self.input_path: str = self.dataset_spec["source_path"]
        self.bronze_output_path: str | None = self.dataset_spec["bronze_path"]
        self.delta_output_path: str = self.dataset_spec["silver_path"]
        self.quarantine_path: str = self.dataset_spec["quarantine_path"]
        self.file_format: str = self.dataset_spec["format"]
        self.column_mapping: dict[str, str] = self.dataset_spec["rename"]
        self.required_columns: list[str] = self.dataset_spec["required_columns"]
        self.partition_cols: list[str] = self.dataset_spec["partition_cols"]
        self.schema_version: str = schema_version
        self.quality_rules: dict[str, Any] = self.dataset_spec["quality_rules"]
        self.source_timezone: str = self.dataset_spec["timezone"]
        self.metadata_path = metadata_path
        self.run_id: str = str(uuid4())

        self.logger = get_logger(f"Ingestor[{dataset_name}]")
        self.ingestion_metrics: dict[str, Any] = {}
        self.last_schema_validation_result: SchemaCompatibilityResult | None = None

    def load_raw_data(self) -> DataFrame:
        """Loads data from source according to specified format."""
        self.logger.info(
            f"Loading raw data from '{self.input_path}' (format: {self.file_format})..."
        )

        if self.file_format == "parquet":
            df = self.spark.read.parquet(self.input_path)
        elif self.file_format == "csv":
            reader = self.spark.read.option("header", "true")
            # Read strings by header name: preserve malformed values in Bronze and
            # avoid positional schema coercion when a CSV header is reordered.
            reader = reader.option("mode", "FAILFAST").option("inferSchema", "false")
            df = reader.csv(self.input_path)
        else:
            raise ValueError(f"Unsupported format: {self.file_format}")

        missing = sorted(set(self.required_columns) - set(df.columns))
        if missing:
            raise ValueError(
                f"{self.dataset_name}: missing required columns: {missing}"
            )
        return df

    def save_to_bronze(self, raw_df: DataFrame) -> None:
        """Persists raw snapshot to Bronze Delta table with exact fidelity and audit timestamp."""
        if not self.bronze_output_path:
            return

        self.logger.info(
            f"Writing raw snapshot to Bronze Delta table at '{self.bronze_output_path}'..."
        )
        sanitized_cols = [
            c.replace(" ", "_").replace("(", "").replace(")", "").replace(";", "")
            for c in raw_df.columns
        ]
        bronze_df = raw_df.toDF(*sanitized_cols).withColumn(
            "_ingestion_timestamp", F.current_timestamp()
        )
        bronze_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(self.bronze_output_path)

    def standardize_column_names(self, df: DataFrame) -> DataFrame:
        """Renames dataset columns to standard snake_case naming conventions and checks casts."""
        self.logger.info("Standardizing column names...")

        def snake(name: str) -> str:
            return re.sub(
                r"[^a-z0-9]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()
            ).strip("_")

        names = [self.column_mapping.get(c, snake(c)) for c in df.columns]
        if len(set(names)) != len(names):
            raise ValueError("Column normalization produces duplicate names")
        df = df.toDF(*names)

        casts = self.dataset_spec["casts"]
        missing_casts = sorted(set(casts) - set(df.columns))
        if missing_casts:
            raise ValueError(
                f"{self.dataset_name}: missing cast columns: {missing_casts}"
            )

        errors = []
        for name, dtype in casts.items():
            if name in df.columns:
                casted = F.col(name).try_cast(dtype)
                errors.append(
                    F.when(
                        F.col(name).isNotNull() & casted.isNull(),
                        F.lit(f"invalid_type:{name}"),
                    )
                )

        if errors:
            df = df.withColumn(
                "_cast_errors",
                F.filter(
                    F.array(*errors).cast("array<string>"), lambda x: x.isNotNull()
                ),
            )
        else:
            df = df.withColumn("_cast_errors", F.array().cast("array<string>"))

        for name, dtype in casts.items():
            if name in df.columns:
                df = df.withColumn(name, F.col(name).try_cast(dtype))
        return df

    def filter_scope(self, df: DataFrame) -> DataFrame:
        """Dataset selection is accounted separately from invalid records."""
        return df

    @abstractmethod
    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        """Performs dataset-specific timestamp normalization and transformations."""

    def classify_records(
        self, df: DataFrame, *, include_reference_checks: bool = True
    ) -> DataFrame:
        """Classify once; valid duplicate survivors use lexicographic payload order.

        This is a deterministic snapshot policy, not correction resolution.
        Invalid rows cannot displace valid rows sharing the same key.
        """
        reference_checks = self.dataset_spec.get("reference_checks", [])
        if include_reference_checks and reference_checks:
            df = apply_reference_checks(self.spark, df, reference_checks)

        parameters = {**self.quality_rules, "source_timezone": self.source_timezone}
        validation_rules = self.dataset_spec["validation"]
        errors = [
            F.when(
                ~F.coalesce(F.expr(sql.format(**parameters)), F.lit(False)), F.lit(name)
            )
            for name, sql in validation_rules.items()
        ]

        keys = self.dataset_spec["key"]
        if not keys:
            raise ValueError("At least one key column is required")

        errors += [F.when(F.col(k).isNull(), F.lit(f"missing_key:{k}")) for k in keys]
        errors += required_value_error_columns(
            df, self.dataset_spec.get("required_values", [])
        )
        failures = F.filter(F.array(*errors), lambda x: x.isNotNull())

        if "_cast_errors" in df.columns:
            failures = F.concat(F.col("_cast_errors"), failures)
        if "_reference_errors" in df.columns:
            failures = F.concat(F.col("_reference_errors"), failures)

        df = df.withColumn("_validation_errors", failures)
        order = F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]))
        ranked = df.withColumn(
            "_duplicate_rank",
            F.row_number().over(
                Window.partitionBy(*keys, F.size("_validation_errors") == 0).orderBy(
                    order
                )
            ),
        )

        return ranked.withColumn(
            "_rejection_reason",
            F.when(
                F.size("_validation_errors") > 0, F.concat_ws(";", "_validation_errors")
            ).when(F.col("_duplicate_rank") > 1, F.lit("duplicate_key")),
        ).drop("_duplicate_rank", "_reference_errors")

    def validate_schema_compatibility(self, df: DataFrame) -> SchemaCompatibilityResult:
        """Validate a prepared batch schema before any Bronze, quarantine, or Silver write."""
        contract = self.dataset_spec.get("schema_compatibility")
        if not contract:
            result = SchemaCompatibilityResult([], [], [], True)
        else:
            result = validate_schema_compatibility(
                (column for column in df.columns if column != "_cast_errors"), contract
            )
        self.last_schema_validation_result = result
        if not result.is_compatible:
            raise ValueError(
                f"{self.dataset_name}: incompatible batch schema: {result.as_dict()}"
            )
        return result

    def split_records(self, classified: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Project the clean schema after rules and keys have used transformed columns."""
        valid = classified.filter(F.col("_rejection_reason").isNull()).drop(
            "_validation_errors", "_cast_errors", "_rejection_reason"
        )

        if "selected_columns" in self.dataset_spec:
            valid = valid.select(*self.dataset_spec["selected_columns"])

        return valid, classified.filter(F.col("_rejection_reason").isNotNull())

    def apply_data_quality_rules(
        self, df: DataFrame, *, include_reference_checks: bool = True
    ) -> tuple[DataFrame, DataFrame]:
        """Apply configured validity predicates and snapshot deduplication."""
        return self.split_records(
            self.classify_records(df, include_reference_checks=include_reference_checks)
        )

    def prepare(self, raw: DataFrame) -> DataFrame:
        """Reusable source preparation, shared by ingestion and benchmarks."""
        return self.normalize_and_transform(
            self.filter_scope(self.standardize_column_names(raw))
        )

    def preflight(self) -> DataFrame:
        """Resolve source, rule, and output schemas before any table is written."""
        prepared = self.prepare(self.load_raw_data())
        self.validate_schema_compatibility(prepared)
        # Reference tables may be created later in the same full-run sequence.
        valid, _ = self.apply_data_quality_rules(
            prepared, include_reference_checks=False
        )
        _ = valid.select(*self.partition_cols).schema
        return valid

    def save_to_delta(self, df: DataFrame, mode: str = "overwrite") -> None:
        """Writes the clean standardized DataFrame as a Silver Delta Lake table."""
        self.logger.info(
            f"Writing clean data to Silver Delta at '{self.delta_output_path}' (mode={mode})..."
        )

        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")

        if self.partition_cols:
            self.logger.info(f"Partitioning by columns: {self.partition_cols}")
            writer = writer.partitionBy(*self.partition_cols)

        writer.save(self.delta_output_path)

    def persist_rejected_records(
        self, rejected_df: DataFrame, run_id: str | None = None
    ) -> None:
        """Append rejected rows with the standard quarantine diagnostics and metadata."""
        (
            rejected_df.withColumn("_run_id", F.lit(run_id or self.run_id))
            .withColumn("_schema_version", F.lit(self.schema_version))
            .withColumn("_rejected_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.quarantine_path)
        )

    def run(self, run_id: str | None = None) -> dict[str, Any]:
        """Executes full medallion ingestion lifecycle."""
        start_time = time.perf_counter()
        self.run_id = run_id or str(uuid4())
        self.logger.info(f"=== Starting Ingestion for {self.dataset_name} ===")

        raw_df = self.load_raw_data().persist(StorageLevel.DISK_ONLY)
        classified = None
        try:
            initial_count = raw_df.count()
            prepared = self.prepare(raw_df)
            self.validate_schema_compatibility(prepared)
            classified = self.classify_records(prepared).persist(
                StorageLevel.DISK_ONLY
            )
            valid_df, rejected_df = self.split_records(classified)
            stats = classified.agg(
                F.count("*").alias("scoped"),
                F.count(F.when(F.col("_rejection_reason").isNull(), 1)).alias("valid"),
                F.count(F.when(F.col("_rejection_reason").isNotNull(), 1)).alias(
                    "rejected"
                ),
                F.count(F.when(F.col("_rejection_reason") == "duplicate_key", 1)).alias(
                    "duplicates"
                ),
            ).first()

            scoped_count, valid_count, rejected_count, duplicate_count = stats
            out_of_scope_count = initial_count - scoped_count
            if out_of_scope_count < 0 or scoped_count != valid_count + rejected_count:
                raise ValueError(f"{self.dataset_name}: row accounting failed")

            self.save_to_bronze(raw_df)

            # Separate per-dataset tables allow heterogeneous rejected schemas.
            self.persist_rejected_records(rejected_df)

            # 6. Persist to Silver Layer
            self.save_to_delta(valid_df)

            duration = round(time.perf_counter() - start_time, 3)
            self.logger.info(
                f"Ingestion completed in {duration}s. "
                f"Processed: {initial_count}, Valid: {valid_count}, Rejected: {rejected_count}"
            )

            self.ingestion_metrics = {
                "dataset_name": self.dataset_name,
                "file_format": self.file_format,
                "schema_version": self.schema_version,
                "initial_records": initial_count,
                "valid_records": valid_count,
                "rejected_records": rejected_count,
                "duplicate_records": duplicate_count,
                "invalid_records": rejected_count - duplicate_count,
                "out_of_scope_records": out_of_scope_count,
                "run_id": self.run_id,
                "quarantine_path": self.quarantine_path,
                "source_timezone": self.source_timezone,
                "duration_seconds": duration,
                "bronze_output_path": self.bronze_output_path,
                "delta_output_path": self.delta_output_path,
                "partition_columns": self.partition_cols,
            }
            audit = {
                k: json.dumps(v) if isinstance(v, (list, dict)) else v
                for k, v in self.ingestion_metrics.items()
            }
            (
                self.spark.range(1)
                .select(
                    *[
                        (F.lit(v).cast("string") if v is None else F.lit(v)).alias(k)
                        for k, v in audit.items()
                    ]
                )
                .withColumn("completed_at", F.current_timestamp())
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(self.metadata_path)
            )
            return self.ingestion_metrics
        finally:
            if classified is not None:
                classified.unpersist()
            raw_df.unpersist()
