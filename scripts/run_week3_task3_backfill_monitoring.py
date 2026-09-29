"""Backfill Task 3 monitoring records from completed Week 3 evidence only."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.common.spark_session import get_spark_session, stop_spark_session
from src.monitoring.pipeline_monitor import PIPELINE_RUNS_PATH, PipelineMonitor


TASK1_EVIDENCE = ROOT / "storage/metrics/week3/task1_incremental_updates.json"
INTEGRATION_EVIDENCE = ROOT / "storage/metrics/week3/task2_integration_refresh.json"
PRODUCT_EVIDENCE = ROOT / "storage/metrics/week3/product_comparison.json"
INCREMENTAL_AUDIT_PATH = ROOT / "storage/delta/metadata/incremental_runs"
GOLD_ROOT = ROOT / "storage/delta/gold"

PRODUCTION_PATHS = (
    "storage/delta/silver/clean_taxi_trips",
    "storage/delta/silver/clean_weather",
    "storage/delta/silver/clean_air_quality",
    "storage/delta/silver/integrated_taxi_trips",
    "storage/delta/gold/daily_mobility_summary",
    "storage/delta/gold/taxi_zone_statistics",
    "storage/delta/gold/weather_impact_summary",
    "storage/delta/gold/air_quality_impact_summary",
)


def read_json(path: Path) -> dict[str, Any]:
    """Read a committed evidence file without modifying it."""
    return json.loads(path.read_text(encoding="utf-8"))


def file_hashes(paths: tuple[Path, ...]) -> dict[Path, str]:
    """Return content hashes used to verify evidence remains read-only."""
    return {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def delta_version(spark: SparkSession, path: str) -> int:
    """Read the current version of an existing production Delta table."""
    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def task1_audit_commit_time(metrics: dict[str, Any]) -> datetime:
    """Find the matching historical incremental audit append commit timestamp."""
    for log_path in sorted((INCREMENTAL_AUDIT_PATH / "_delta_log").glob("*.json")):
        lines = [json.loads(line) for line in log_path.read_text().splitlines()]
        commit = next((line["commitInfo"] for line in lines if "commitInfo" in line), None)
        if not commit:
            continue
        for line in lines:
            if "add" not in line or "stats" not in line["add"]:
                continue
            stats = json.loads(line["add"]["stats"])
            values = stats.get("minValues", {})
            if all(
                values.get(name) == metrics[name]
                for name in (
                    "dataset",
                    "execution_seconds",
                    "input_records",
                    "inserted_records",
                    "duplicate_records",
                    "rejected_records",
                )
            ):
                return datetime.fromtimestamp(
                    commit["timestamp"] / 1000, tz=timezone.utc
                )
    raise ValueError(f"No incremental audit commit matches {metrics['dataset']}")


def delta_commit_time(path: Path, version: int) -> datetime:
    """Read the commit timestamp for the recorded Delta output version."""
    log_path = path / "_delta_log" / f"{version:020d}.json"
    for line in log_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if "commitInfo" in payload:
            return datetime.fromtimestamp(
                payload["commitInfo"]["timestamp"] / 1000, tz=timezone.utc
            )
    raise ValueError(f"Missing commitInfo in {log_path}")


def historical_start(completed_at: datetime, execution_seconds: float) -> datetime:
    """Reconstruct the recorded start from a completion timestamp and measured duration."""
    return completed_at - timedelta(seconds=execution_seconds)


def already_backfilled(spark: SparkSession, pipeline_run_id: str) -> bool:
    """Check deterministic historical identity before appending a monitoring row."""
    if not DeltaTable.isDeltaTable(spark, PIPELINE_RUNS_PATH):
        return False
    return (
        spark.read.format("delta")
        .load(PIPELINE_RUNS_PATH)
        .filter(F.col("pipeline_run_id") == pipeline_run_id)
        .limit(1)
        .count()
        == 1
    )


def append_historical_record(
    spark: SparkSession,
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    pipeline_kind: str,
    dataset_or_product: str,
    started_at: datetime,
    completed_at: datetime,
    metrics: dict[str, Any],
    fields: dict[str, Any],
) -> bool:
    """Append a successful historical record once, using deterministic identity."""
    if already_backfilled(spark, pipeline_run_id):
        return False
    execution = PipelineMonitor(
        spark, pipeline_name, pipeline_kind, dataset_or_product
    ).start_execution(
        pipeline_run_id=pipeline_run_id,
        started_at=started_at,
        completed_at=completed_at,
        input_path=fields.pop("input_path"),
        target_path=fields.pop("target_path"),
    )
    if not execution.finalize_success(metrics, **fields):
        raise RuntimeError(f"Could not append historical record {pipeline_run_id}")
    return True


def task1_records(task1: dict[str, Any]) -> list[dict[str, Any]]:
    """Map the three completed Task 1 incremental executions from their evidence."""
    sources = (
        (
            "Taxi Trips",
            "taxi_trips",
            "week3_taxi_incremental",
            "data/updates/yellow_tripdata_2024_update01.parquet",
            "storage/delta/silver/clean_taxi_trips",
        ),
        (
            "Weather",
            "weather",
            "week3_weather_incremental",
            "data/updates/weather_update.csv",
            "storage/delta/silver/clean_weather",
        ),
        (
            "Air Quality",
            "air_quality",
            "week3_air_quality_incremental",
            "data/updates/air_quality_update.csv",
            "storage/delta/silver/clean_air_quality",
        ),
    )
    records = []
    for evidence_name, dataset, pipeline_name, input_path, target_path in sources:
        metrics = task1["datasets"][evidence_name]["incremental_ingestion"]
        audit_metrics = {**metrics, "dataset": dataset}
        completed_at = task1_audit_commit_time(audit_metrics)
        records.append(
            {
                "pipeline_run_id": f"backfill:task1_incremental_updates:{dataset}",
                "pipeline_name": pipeline_name,
                "pipeline_kind": "incremental_ingestion",
                "dataset_or_product": dataset,
                "started_at": historical_start(
                    completed_at, metrics["execution_seconds"]
                ),
                "completed_at": completed_at,
                "metrics": metrics,
                "fields": {
                    "input_path": input_path,
                    "target_path": target_path,
                    "validation_failure_count": metrics["rejected_records"],
                    "schema_changes": json.dumps(metrics["schema_changes"]),
                    "execution_seconds": metrics["execution_seconds"],
                    "delta_version_before": metrics["delta_version_before"],
                    "delta_version_after": metrics["delta_version_after"],
                },
            }
        )
    return records


def integration_record(integration: dict[str, Any]) -> dict[str, Any]:
    """Map the completed integrated Taxi refresh from its Task 2 evidence."""
    execution = integration["execution"]
    scope = integration["incremental_scope"]
    schema = integration["schema_evolution"]
    completed_at = datetime.fromisoformat(integration["created_at_utc"])
    return {
        "pipeline_run_id": "backfill:task2_integration_refresh:integrated_taxi_trips",
        "pipeline_name": "week3_task2_incremental_integration",
        "pipeline_kind": "integration",
        "dataset_or_product": "integrated_taxi_trips",
        "started_at": historical_start(completed_at, execution["execution_seconds"]),
        "completed_at": completed_at,
        "metrics": execution,
        "fields": {
            "input_path": "storage/delta/silver/clean_taxi_trips",
            "target_path": integration["baseline"]["table"],
            "processed_records": scope["new_taxi_candidates"],
            "updated_records": 0,
            "duplicate_records": scope["duplicate_trip_ids_ignored"],
            "rejected_records": scope["rejected_or_unsupported_rows"],
            "schema_version_before": schema["integrated_schema_version_before"],
            "schema_version_after": schema["integrated_schema_version_after"],
            "schema_changes": json.dumps(schema["delta_schema_changes"]),
            "execution_seconds": execution["execution_seconds"],
            "delta_version_before": execution["delta_version_before"],
            "delta_version_after": execution["delta_version_after"],
        },
    }


def product_records(products: dict[str, Any]) -> list[dict[str, Any]]:
    """Map the four completed Gold refreshes from their per-product evidence."""
    scope_fields = {
        "daily_mobility_summary": "new_aggregate_rows",
        "taxi_zone_statistics": "new_aggregate_rows",
        "weather_impact_summary": "updated_zone_weather_groups",
        "air_quality_impact_summary": "incremental_hour_rows",
    }
    records = []
    for name, product in products["products"].items():
        completed_at = delta_commit_time(
            GOLD_ROOT / name, product["updated_delta_version"]
        )
        execution_seconds = product["incremental_refresh_time_seconds"]
        fields: dict[str, Any] = {
            "input_path": "storage/delta/silver/integrated_taxi_trips",
            "target_path": str(GOLD_ROOT / name),
            "processed_records": product["affected_scope"][scope_fields[name]],
            "schema_changes": json.dumps(product["schema_evolution_fields_used"]),
            "execution_seconds": execution_seconds,
            "delta_version_before": product["baseline_delta_version"],
            "delta_version_after": product["updated_delta_version"],
        }
        records.append(
            {
                "pipeline_run_id": f"backfill:product_comparison:{name}",
                "pipeline_name": "week3_task2_incremental_products",
                "pipeline_kind": "gold_refresh",
                "dataset_or_product": name,
                "started_at": historical_start(completed_at, execution_seconds),
                "completed_at": completed_at,
                "metrics": {},
                "fields": fields,
            }
        )
    return records


def monitoring_summary(spark: SparkSession) -> dict[str, Any]:
    """Return the requested aggregate checks for the monitoring table."""
    table = spark.read.format("delta").load(PIPELINE_RUNS_PATH)
    totals = table.agg(
        F.count("*").alias("total_rows"),
        F.min("execution_seconds").alias("minimum_execution_seconds"),
        F.max("execution_seconds").alias("maximum_execution_seconds"),
        F.sum("rejected_records").alias("total_rejected_records"),
        F.sum("validation_failure_count").alias("total_validation_failures"),
    ).first().asDict()
    return {
        **totals,
        "rows_by_pipeline_kind": [
            row.asDict()
            for row in table.groupBy("pipeline_kind").count().orderBy("pipeline_kind").collect()
        ],
        "rows_by_status": [
            row.asDict()
            for row in table.groupBy("status").count().orderBy("status").collect()
        ],
    }


def run(spark: SparkSession) -> dict[str, Any]:
    """Backfill eight evidence-backed records and verify read-only inputs stayed unchanged."""
    evidence_paths = (TASK1_EVIDENCE, INTEGRATION_EVIDENCE, PRODUCT_EVIDENCE)
    evidence_before = file_hashes(evidence_paths)
    production_before = {path: delta_version(spark, path) for path in PRODUCTION_PATHS}

    records = [
        *task1_records(read_json(TASK1_EVIDENCE)),
        integration_record(read_json(INTEGRATION_EVIDENCE)),
        *product_records(read_json(PRODUCT_EVIDENCE)),
    ]
    inserted = 0
    skipped = 0
    for record in records:
        if append_historical_record(spark, **record):
            inserted += 1
        else:
            skipped += 1

    production_after = {path: delta_version(spark, path) for path in PRODUCTION_PATHS}
    if production_after != production_before:
        raise RuntimeError("A Task 1 or Task 2 production Delta version changed")
    if file_hashes(evidence_paths) != evidence_before:
        raise RuntimeError("A Week 3 evidence file changed during backfill")

    return {
        "expected_historical_records": len(records),
        "inserted_records": inserted,
        "skipped_existing_records": skipped,
        "production_delta_versions_unchanged": True,
        "evidence_files_unchanged": True,
        "monitoring": monitoring_summary(spark),
    }


def main() -> None:
    spark = get_spark_session(app_name="UrbanDataPlatform_Week3_Task3_Backfill")
    try:
        print(json.dumps(run(spark), indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
