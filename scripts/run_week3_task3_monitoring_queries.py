"""Run the Week 3 Task 3 Spark SQL monitoring analyses."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.common.spark_session import get_spark_session, stop_spark_session
from src.monitoring.pipeline_monitor import PIPELINE_RUNS_PATH


OUTPUT_PATH = ROOT / "storage/metrics/week3/monitoring_queries.json"
VIEW_NAME = "monitoring_pipeline_runs"
EXPECTED_MONITORING_ROWS = 8
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
EVIDENCE_PATHS = (
    ROOT / "storage/metrics/week3/task1_incremental_updates.json",
    ROOT / "storage/metrics/week3/task2_integration_refresh.json",
    ROOT / "storage/metrics/week3/product_comparison.json",
)


def delta_versions(spark: SparkSession) -> dict[str, int]:
    """Read production Delta versions without changing any table."""
    return {
        path: int(DeltaTable.forPath(spark, path).history(1).first()["version"])
        for path in PRODUCTION_PATHS
    }


def evidence_hashes() -> dict[Path, str]:
    """Hash immutable Task 1/Task 2 evidence before and after query execution."""
    return {
        path: hashlib.sha256(path.read_bytes()).hexdigest() for path in EVIDENCE_PATHS
    }


def rows(frame: Any) -> list[dict[str, Any]]:
    """Collect SQL result rows while preserving Spark-returned scalar values."""
    return [row.asDict(recursive=True) for row in frame.collect()]


def run(spark: SparkSession) -> dict[str, Any]:
    """Execute the four required monitoring queries against one temporary SQL view."""
    production_before = delta_versions(spark)
    evidence_before = evidence_hashes()
    monitoring = spark.read.format("delta").load(PIPELINE_RUNS_PATH)
    monitoring.createOrReplaceTempView(VIEW_NAME)

    total_monitoring_rows = spark.sql(
        f"SELECT COUNT(*) AS total_rows FROM {VIEW_NAME}"
    ).first()["total_rows"]
    if total_monitoring_rows != EXPECTED_MONITORING_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_MONITORING_ROWS} monitoring rows, found "
            f"{total_monitoring_rows}"
        )

    query_1 = rows(
        spark.sql(
            f"""
            SELECT
                dataset_or_product,
                SUM(validation_failure_count) AS total_validation_failure_count
            FROM {VIEW_NAME}
            GROUP BY dataset_or_product
            ORDER BY total_validation_failure_count DESC NULLS LAST, dataset_or_product
            """
        )
    )
    query_2 = rows(
        spark.sql(
            f"""
            SELECT
                pipeline_run_id,
                pipeline_name,
                pipeline_kind,
                dataset_or_product,
                execution_seconds,
                status
            FROM {VIEW_NAME}
            ORDER BY execution_seconds DESC NULLS LAST, started_at, pipeline_run_id
            LIMIT 1
            """
        )
    )
    query_3 = rows(
        spark.sql(
            f"""
            SELECT
                pipeline_run_id,
                pipeline_name,
                dataset_or_product,
                rejected_records,
                started_at,
                status
            FROM {VIEW_NAME}
            ORDER BY started_at, pipeline_run_id
            """
        )
    )
    query_4 = rows(
        spark.sql(
            f"""
            SELECT
                started_at,
                pipeline_name,
                pipeline_kind,
                dataset_or_product,
                execution_seconds
            FROM {VIEW_NAME}
            ORDER BY started_at, pipeline_run_id
            """
        )
    )

    production_after = delta_versions(spark)
    if production_after != production_before:
        raise RuntimeError("A production Delta table changed during monitoring queries")
    if evidence_hashes() != evidence_before:
        raise RuntimeError("A Task 1 or Task 2 evidence file changed during monitoring queries")

    evidence = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "monitoring_table_path": PIPELINE_RUNS_PATH,
        "total_monitoring_rows": total_monitoring_rows,
        "query_1_most_validation_failures": query_1,
        "query_2_longest_processing_time": query_2,
        "query_3_rejected_records_per_execution": query_3,
        "query_4_processing_time_trend": query_4,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(evidence, indent=2, default=str) + "\n", encoding="utf-8")
    return evidence


def main() -> None:
    spark = get_spark_session(app_name="UrbanDataPlatform_Week3_Task3_Monitoring")
    try:
        print(json.dumps(run(spark), indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
