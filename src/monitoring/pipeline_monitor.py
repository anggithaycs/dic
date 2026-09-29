"""Append-only Delta monitoring records for heterogeneous pipeline executions."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.logger import get_logger


PIPELINE_RUNS_PATH = "storage/delta/metadata/pipeline_runs"

PIPELINE_RUNS_SCHEMA = StructType(
    [
        StructField("monitoring_run_id", StringType(), nullable=False),
        StructField("pipeline_run_id", StringType(), nullable=True),
        StructField("pipeline_name", StringType(), nullable=False),
        StructField("pipeline_kind", StringType(), nullable=False),
        StructField("dataset_or_product", StringType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("completed_at", TimestampType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("execution_seconds", DoubleType(), nullable=False),
        StructField("processed_records", LongType(), nullable=True),
        StructField("inserted_records", LongType(), nullable=True),
        StructField("updated_records", LongType(), nullable=True),
        StructField("duplicate_records", LongType(), nullable=True),
        StructField("rejected_records", LongType(), nullable=True),
        StructField("validation_failure_count", LongType(), nullable=True),
        StructField("validation_failures_json", StringType(), nullable=True),
        StructField("schema_version_before", StringType(), nullable=True),
        StructField("schema_version_after", StringType(), nullable=True),
        StructField("schema_changes", StringType(), nullable=True),
        StructField("input_path", StringType(), nullable=True),
        StructField("target_path", StringType(), nullable=True),
        StructField("delta_version_before", LongType(), nullable=True),
        StructField("delta_version_after", LongType(), nullable=True),
        StructField("error_type", StringType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
    ]
)

_RECORD_FIELDS = frozenset(field.name for field in PIPELINE_RUNS_SCHEMA.fields)
_CALLER_FIELDS = _RECORD_FIELDS - {
    "monitoring_run_id",
    "pipeline_run_id",
    "pipeline_name",
    "pipeline_kind",
    "dataset_or_product",
    "started_at",
    "completed_at",
    "status",
    "execution_seconds",
}
_METRIC_ALIASES = {
    "processed_records": ("processed_records", "input_records", "initial_records", "total_records"),
    "inserted_records": ("inserted_records", "inserted_rows"),
    "updated_records": ("updated_records", "updated_rows"),
    "duplicate_records": ("duplicate_records",),
    "rejected_records": ("rejected_records",),
    "validation_failure_count": ("validation_failure_count", "invalid_records"),
    "schema_version_before": ("schema_version_before",),
    "schema_version_after": ("schema_version_after", "schema_version"),
    "schema_changes": ("schema_changes",),
    "input_path": ("input_path",),
    "target_path": ("target_path", "output_path", "delta_output_path"),
    "delta_version_before": ("delta_version_before",),
    "delta_version_after": ("delta_version_after",),
}


def _first_value(metrics: Mapping[str, Any], names: tuple[str, ...]) -> Any:
    """Return the first present metric value without manufacturing a default."""
    for name in names:
        if name in metrics:
            return metrics[name]
    return None


def _validation_failures_json(value: Any) -> str | None:
    """Serialize structured validation failures while preserving absent values as null."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str)


@dataclass
class PipelineExecution:
    """A started execution that can be finalized once as success or failure."""

    monitor: "PipelineMonitor"
    monitoring_run_id: str
    pipeline_run_id: str | None
    started_at: datetime
    started_perf_counter: float
    completed_at: datetime | None = None
    context: dict[str, Any] = field(default_factory=dict)
    _finalized: bool = field(default=False, init=False, repr=False)

    def finalize_success(
        self,
        metrics: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> bool:
        """Append this execution as successful, returning whether the write succeeded."""
        return self._finalize("success", metrics=metrics, fields=fields)

    def finalize_failure(
        self,
        error: BaseException,
        metrics: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> bool:
        """Append this execution as failed without re-raising the pipeline exception."""
        fields.setdefault("error_type", type(error).__name__)
        fields.setdefault("error_message", str(error))
        return self._finalize("failed", metrics=metrics, fields=fields)

    def _finalize(
        self,
        status: str,
        metrics: Mapping[str, Any] | None,
        fields: Mapping[str, Any],
    ) -> bool:
        if self._finalized:
            self.monitor.logger.warning(
                "Ignoring duplicate monitoring finalization for %s", self.monitoring_run_id
            )
            return False
        self._finalized = True
        return self.monitor._append_record(
            execution=self,
            status=status,
            metrics=metrics or {},
            fields=fields,
        )


class PipelineMonitor:
    """Create append-only Delta records for pipeline execution outcomes."""

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        pipeline_kind: str,
        dataset_or_product: str | None = None,
        table_path: str = PIPELINE_RUNS_PATH,
    ) -> None:
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.pipeline_kind = pipeline_kind
        self.dataset_or_product = dataset_or_product
        self.table_path = table_path
        self.logger = get_logger("PipelineMonitor")

    def start_execution(
        self,
        pipeline_run_id: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        **context: Any,
    ) -> PipelineExecution:
        """Capture execution bounds; historical callers may provide recorded UTC times."""
        return PipelineExecution(
            monitor=self,
            monitoring_run_id=uuid4().hex,
            pipeline_run_id=pipeline_run_id,
            started_at=started_at or datetime.now(timezone.utc),
            started_perf_counter=time.perf_counter(),
            completed_at=completed_at,
            context=self._known_fields(context),
        )

    def _append_record(
        self,
        execution: PipelineExecution,
        status: str,
        metrics: Mapping[str, Any],
        fields: Mapping[str, Any],
    ) -> bool:
        """Build and append one record, logging rather than propagating monitor failures."""
        completed_at = execution.completed_at or datetime.now(timezone.utc)
        record: dict[str, Any] = {
            "monitoring_run_id": execution.monitoring_run_id,
            "pipeline_run_id": execution.pipeline_run_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_kind": self.pipeline_kind,
            "dataset_or_product": self.dataset_or_product,
            "started_at": execution.started_at,
            "completed_at": completed_at,
            "status": status,
            "execution_seconds": round(
                time.perf_counter() - execution.started_perf_counter, 3
            ),
            "error_type": None,
            "error_message": None,
        }
        for target, aliases in _METRIC_ALIASES.items():
            record[target] = _first_value(metrics, aliases)
        record["validation_failures_json"] = _validation_failures_json(
            metrics.get("validation_failures")
        )
        record.update(execution.context)
        record.update(self._known_fields(fields, allow_execution_seconds=True))
        record["validation_failures_json"] = _validation_failures_json(
            record.get("validation_failures_json")
        )

        try:
            self.spark.createDataFrame([record], schema=PIPELINE_RUNS_SCHEMA).write.format(
                "delta"
            ).mode("append").save(self.table_path)
        except Exception:
            self.logger.exception(
                "Could not append monitoring record %s to %s; pipeline outcome is unchanged",
                execution.monitoring_run_id,
                self.table_path,
            )
            return False
        return True

    @staticmethod
    def _known_fields(
        values: Mapping[str, Any], allow_execution_seconds: bool = False
    ) -> dict[str, Any]:
        """Retain declared caller fields while reserving lifecycle fields for the monitor."""
        allowed = _CALLER_FIELDS | (
            {"execution_seconds"} if allow_execution_seconds else set()
        )
        return {key: value for key, value in values.items() if key in allowed}
