"""Measure isolated Week 3 validation, monitoring, and storage overhead."""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.base_ingestion import BaseDatasetIngestor
from src.monitoring.pipeline_monitor import PipelineMonitor


OUTPUT_PATH = ROOT / "storage/metrics/week3/task5_evaluation.json"
TASK1_EVIDENCE = ROOT / "storage/metrics/week3/task1_incremental_updates.json"
TASK2_INTEGRATION_EVIDENCE = ROOT / "storage/metrics/week3/task2_integration_refresh.json"
PRODUCT_EVIDENCE = ROOT / "storage/metrics/week3/product_comparison.json"
ANALYTICAL_EVIDENCE = ROOT / "storage/metrics/week3/analytical_refresh.json"
IMMUTABLE_EVIDENCE = (
    TASK1_EVIDENCE,
    TASK2_INTEGRATION_EVIDENCE,
    PRODUCT_EVIDENCE,
    ANALYTICAL_EVIDENCE,
    ROOT / "storage/metrics/week3/monitoring_queries.json",
    ROOT / "storage/metrics/week3/task4_validation.json",
)
PRODUCTION_DELTA_ROOTS = (
    ROOT / "storage/delta/bronze",
    ROOT / "storage/delta/silver",
    ROOT / "storage/delta/gold",
)
PIPELINE_RUNS_PATH = "storage/delta/metadata/pipeline_runs"
VALIDATION_ROWS = 120_000
MONITORING_ROWS = 300_000


class EvaluationIngestor(BaseDatasetIngestor):
    """Identity ingestor used only with synthetic Task 5 inputs."""

    def __init__(self, spark: SparkSession, spec: dict[str, Any]) -> None:
        super().__init__(
            spark=spark,
            dataset_name="Task5Evaluation",
            dataset_spec=spec,
            schema_version="task5-evaluation",
            metadata_path=spec["metadata_path"],
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        return df


def allocated_bytes(path: Path) -> int:
    """Return allocated local disk bytes for a path without modifying it."""
    if not path.exists():
        return 0
    paths = [path]
    if path.is_dir():
        paths.extend(path.rglob("*"))
    return sum(item.stat().st_blocks * 512 for item in paths)


def production_versions(spark: SparkSession) -> dict[str, int]:
    """Read Delta versions without changing any production table."""
    versions: dict[str, int] = {}
    for root in PRODUCTION_DELTA_ROOTS:
        for delta_log in root.rglob("_delta_log"):
            table_path = delta_log.parent
            relative_path = str(table_path.relative_to(ROOT))
            versions[relative_path] = int(
                DeltaTable.forPath(spark, str(table_path)).history(1).first()["version"]
            )
    return versions


def evidence_hashes() -> dict[str, str]:
    """Hash immutable Task 1–4 evidence files."""
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in IMMUTABLE_EVIDENCE
    }


def summary(samples: list[float]) -> dict[str, Any]:
    """Return rounded samples plus mean and median for a measured group."""
    return {
        "samples_seconds": [round(value, 6) for value in samples],
        "mean_seconds": round(statistics.mean(samples), 6),
        "median_seconds": round(statistics.median(samples), 6),
    }


def overhead(baseline: list[float], measured: list[float]) -> dict[str, float]:
    """Calculate mean and median absolute/percentage overhead from paired groups."""
    baseline_mean = statistics.mean(baseline)
    measured_mean = statistics.mean(measured)
    baseline_median = statistics.median(baseline)
    measured_median = statistics.median(measured)
    return {
        "mean_absolute_overhead_seconds": round(measured_mean - baseline_mean, 6),
        "median_absolute_overhead_seconds": round(
            measured_median - baseline_median, 6
        ),
        "mean_percentage_overhead": round(
            ((measured_mean - baseline_mean) / baseline_mean) * 100, 6
        ),
        "median_percentage_overhead": round(
            ((measured_median - baseline_median) / baseline_median) * 100, 6
        ),
    }


def validation_spec(root: Path, reference_path: str) -> dict[str, Any]:
    """Define synthetic data rules that exercise all row-level validation families."""
    return {
        "source_path": "synthetic",
        "bronze_path": None,
        "silver_path": str(root / "silver"),
        "quarantine_path": str(root / "quarantine"),
        "metadata_path": str(root / "metadata"),
        "format": "csv",
        "timezone": "UTC",
        "partition_cols": [],
        "required_columns": ["business_id", "required_text", "zone_id", "amount"],
        "rename": {},
        "casts": {},
        "key": ["business_id"],
        "quality_rules": {},
        "validation": {"valid_amount": "amount >= 0"},
        "selected_columns": ["business_id", "required_text", "zone_id", "amount"],
        "required_values": ["required_text"],
        "reference_checks": [
            {
                "source_field": "zone_id",
                "reference_path": reference_path,
                "reference_field": "location_id",
                "rule_name": "missing_reference:zone_id",
            }
        ],
    }


def validation_benchmark(spark: SparkSession, root: Path) -> dict[str, Any]:
    """Measure classifier cost over a cached, prepared synthetic DataFrame."""
    reference_path = str(root / "zone_reference")
    spark.createDataFrame([(1,), (2,)], ["location_id"]).write.format("delta").save(
        reference_path
    )
    raw = (
        spark.range(VALIDATION_ROWS)
        .select(
            F.when((F.col("id") % 20) < 2, F.floor(F.col("id") / 20))
            .otherwise(F.col("id") + F.lit(VALIDATION_ROWS))
            .cast("long")
            .alias("business_id"),
            F.when((F.col("id") % 31) == 0, F.lit(" "))
            .otherwise(F.lit("present"))
            .alias("required_text"),
            F.when((F.col("id") % 29) == 0, F.lit(999))
            .otherwise((F.col("id") % 2) + F.lit(1))
            .cast("long")
            .alias("zone_id"),
            F.when((F.col("id") % 23) == 0, F.lit(-1.0))
            .otherwise(F.lit(1.0))
            .alias("amount"),
        )
        .persist()
    )
    ingestor = EvaluationIngestor(spark, validation_spec(root, reference_path))
    prepared = ingestor.prepare(raw).persist()
    try:
        input_rows = prepared.count()  # materialize preparation outside timed groups

        def baseline_once() -> tuple[float, int]:
            started = time.perf_counter()
            count = prepared.select("business_id", "required_text", "zone_id", "amount").count()
            return time.perf_counter() - started, count

        def validation_once() -> tuple[float, int, int]:
            classified = ingestor.classify_records(prepared).persist()
            try:
                started = time.perf_counter()
                accepted, rejected = ingestor.split_records(classified)
                accepted_count = accepted.count()
                rejected_count = rejected.count()
                return time.perf_counter() - started, accepted_count, rejected_count
            finally:
                classified.unpersist()

        # One warm-up of each group; neither timing is retained.
        baseline_once()
        _, accepted_rows, rejected_rows = validation_once()

        baseline_samples: list[float] = []
        validation_samples: list[float] = []
        for index in range(5):
            if index % 2 == 0:
                baseline_time, _ = baseline_once()
                validation_time, accepted, rejected = validation_once()
            else:
                validation_time, accepted, rejected = validation_once()
                baseline_time, _ = baseline_once()
            if (accepted, rejected) != (accepted_rows, rejected_rows):
                raise RuntimeError("Validation benchmark row counts changed between repetitions")
            baseline_samples.append(baseline_time)
            validation_samples.append(validation_time)
        return {
            "methodology": (
                "Prepared synthetic input was cached and materialized once. Baseline counts "
                "the same prepared columns without classification; validation uses the reusable "
                "classify_records and split_records path. Quarantine writes are excluded."
            ),
            "input_row_count": input_rows,
            "accepted_row_count": accepted_rows,
            "rejected_row_count": rejected_rows,
            "warmup_count": 1,
            "measured_repetitions": 5,
            "baseline": summary(baseline_samples),
            "validation": summary(validation_samples),
            **overhead(baseline_samples, validation_samples),
            "primary_metric": "median_absolute_overhead_seconds",
        }
    finally:
        prepared.unpersist()
        raw.unpersist()


def monitoring_benchmark(spark: SparkSession, root: Path) -> dict[str, Any]:
    """Measure real PipelineMonitor finalization against a fixed cached operation."""
    monitoring_path = str(root / "monitoring_runs")
    workload = (
        spark.range(MONITORING_ROWS)
        .select((F.col("id") % 97).alias("bucket"), F.col("id").alias("value"))
        .persist()
    )
    workload.count()

    def operation() -> None:
        workload.filter(F.col("bucket") < 50).agg(F.sum("value").alias("total")).first()

    def baseline_once() -> float:
        started = time.perf_counter()
        operation()
        return time.perf_counter() - started

    def monitored_once(run_id: str) -> float:
        monitor = PipelineMonitor(
            spark,
            pipeline_name="task5_monitoring_overhead",
            pipeline_kind="benchmark",
            dataset_or_product="synthetic_operation",
            table_path=monitoring_path,
        )
        started = time.perf_counter()
        execution = monitor.start_execution(run_id, input_path="synthetic")
        operation()
        if not execution.finalize_success({"processed_records": MONITORING_ROWS}):
            raise RuntimeError("Temporary monitoring record could not be written")
        return time.perf_counter() - started

    try:
        baseline_once()  # baseline warm-up
        monitored_once("warmup")  # creates and warms the temporary Delta table
        baseline_samples: list[float] = []
        monitored_samples: list[float] = []
        for index in range(10):
            if index % 2 == 0:
                baseline_samples.append(baseline_once())
                monitored_samples.append(monitored_once(f"measured-{index}"))
            else:
                monitored_samples.append(monitored_once(f"measured-{index}"))
                baseline_samples.append(baseline_once())
        temporary_rows = spark.read.format("delta").load(monitoring_path).count()
        return {
            "methodology": (
                "A cached synthetic aggregation is measured alone and with real PipelineMonitor "
                "start/finalize plus one append to a pre-created temporary Delta table."
            ),
            "synthetic_operation_input_row_count": MONITORING_ROWS,
            "warmup_count": 1,
            "measured_repetitions": 10,
            "baseline": summary(baseline_samples),
            "monitored": summary(monitored_samples),
            **overhead(baseline_samples, monitored_samples),
            "primary_metric": "median_absolute_overhead_seconds",
            "measured_monitoring_rows_written": 10,
            "temporary_monitoring_rows_including_warmup": temporary_rows,
        }
    finally:
        workload.unpersist()


def storage_snapshot() -> dict[str, Any]:
    """Capture allocated local byte sizes and the stated operational-overhead ratio."""
    paths = {
        "bronze": ROOT / "storage/delta/bronze",
        "silver": ROOT / "storage/delta/silver",
        "gold": ROOT / "storage/delta/gold",
        "pipeline_runs": ROOT / "storage/delta/metadata/pipeline_runs",
        "incremental_runs": ROOT / "storage/delta/metadata/incremental_runs",
        "ingestion_runs": ROOT / "storage/delta/metadata/ingestion_runs",
        "quarantine": ROOT / "storage/delta/quarantine",
        "week3_metrics": ROOT / "storage/metrics/week3",
    }
    sizes = {name: allocated_bytes(path) for name, path in paths.items()}
    core = sizes["bronze"] + sizes["silver"] + sizes["gold"]
    week3_metadata = sizes["pipeline_runs"] + sizes["incremental_runs"]
    return {
        "measurement": "allocated local filesystem bytes (st_blocks * 512)",
        "raw_path_sizes_bytes": sizes,
        "core_data_bytes": core,
        "week3_operational_metadata_bytes": week3_metadata,
        "operational_storage_overhead_percent": round(
            (week3_metadata / core) * 100, 6
        ),
        "evidence_artifact_bytes": sizes["week3_metrics"],
        "quarantine_bytes": sizes["quarantine"],
        "notes": (
            "Week 3 operational metadata includes pipeline_runs and incremental_runs only. "
            "Quarantine and ingestion_runs predate Week 3 and are reported separately."
        ),
    }


def existing_performance_evidence() -> dict[str, Any]:
    """Copy previously measured Task 1/Task 2 values without recomputation."""
    task1 = json.loads(TASK1_EVIDENCE.read_text(encoding="utf-8"))
    integration = json.loads(TASK2_INTEGRATION_EVIDENCE.read_text(encoding="utf-8"))
    products = json.loads(PRODUCT_EVIDENCE.read_text(encoding="utf-8"))
    analytical = json.loads(ANALYTICAL_EVIDENCE.read_text(encoding="utf-8"))
    return {
        "incremental_update_times": {
            name: dataset["incremental_ingestion"]["execution_seconds"]
            for name, dataset in task1["datasets"].items()
        },
        "analytical_refresh_times": {
            "integrated_taxi_trips": integration["execution"]["execution_seconds"],
            **{
                name: product["incremental_refresh_time_seconds"]
                for name, product in products["products"].items()
            },
        },
        "analytical_full_refresh_comparison": {
            name: {
                "full_refresh_seconds": product["full_refresh_seconds"],
                "refresh_savings": product["refresh_savings"],
                "speedup": product["speedup"],
                "equivalent": product["equivalent"],
            }
            for name, product in analytical["products"].items()
        },
    }


def run(spark: SparkSession) -> dict[str, Any]:
    """Run isolated benchmarks and preserve production-integrity evidence."""
    versions_before = production_versions(spark)
    monitoring_rows_before = spark.read.format("delta").load(PIPELINE_RUNS_PATH).count()
    hashes_before = evidence_hashes()
    performance = existing_performance_evidence()
    with tempfile.TemporaryDirectory(prefix="week3_task5_") as directory:
        temp_root = Path(directory)
        validation = validation_benchmark(spark, temp_root / "validation")
        monitoring = monitoring_benchmark(spark, temp_root / "monitoring")

    versions_after = production_versions(spark)
    monitoring_rows_after = spark.read.format("delta").load(PIPELINE_RUNS_PATH).count()
    hashes_after = evidence_hashes()
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_environment": {
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "validation_rows": VALIDATION_ROWS,
            "monitoring_operation_rows": MONITORING_ROWS,
            "local_benchmark": True,
            "temporary_paths_cleaned": True,
        },
        "methodology_summary": (
            "Task 1 and Task 2 timings are copied from existing evidence. New benchmarks use "
            "only synthetic cached DataFrames and temporary Delta paths."
        ),
        **performance,
        "validation_overhead": validation,
        "monitoring_overhead": monitoring,
        "storage_snapshot": storage_snapshot(),
        "production_integrity": {
            "production_delta_versions_before": versions_before,
            "production_delta_versions_after": versions_after,
            "production_delta_versions_unchanged": versions_before == versions_after,
            "production_monitoring_row_count_before": monitoring_rows_before,
            "production_monitoring_row_count_after": monitoring_rows_after,
            "production_monitoring_row_count_unchanged": monitoring_rows_before
            == monitoring_rows_after,
            "task1_to_task4_evidence_hashes_unchanged": hashes_before == hashes_after,
        },
    }


def main() -> None:
    spark = get_spark_session(
        app_name="Week3Task5Evaluation", master="local[2]", shuffle_partitions=4
    )
    try:
        evidence = run(spark)
        # Create the evidence before sizing its containing directory, then refresh the snapshot.
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        evidence["storage_snapshot"] = storage_snapshot()
        OUTPUT_PATH.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        print(json.dumps(evidence, indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
