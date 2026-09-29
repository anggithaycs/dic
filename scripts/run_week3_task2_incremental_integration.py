"""Incrementally extend the integrated Silver table for Week 3 Task 2."""

from __future__ import annotations

import json
import os
import sys
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

from src.common.config import load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.monitoring.pipeline_monitor import PipelineMonitor

EVIDENCE_PATH = ROOT / "storage/metrics/week3/task2_integration_refresh.json"
BASELINE_EVIDENCE_PATH = ROOT / "storage/metrics/week3/task2_baseline.json"
EXPECTED_BASELINE_VERSION = 0
EXPECTED_BASELINE_ROWS = 9_394_330
EXPECTED_SOURCE_VERSIONS = {
    "taxi": 1,
    "weather": 2,
    "air_quality": 2,
    "zones": 0,
}
GOLD_PRODUCTS = (
    "daily_mobility_summary",
    "taxi_zone_statistics",
    "weather_impact_summary",
    "air_quality_impact_summary",
)


def current_version(spark: SparkSession, path: str) -> int:
    return int(
        DeltaTable.forPath(spark, path).history(1).select("version").first()["version"]
    )


def table_count(frame: DataFrame) -> int:
    return int(frame.count())


def hash_rows(frame: DataFrame, columns: list[str]) -> DataFrame:
    return frame.select(
        "trip_id",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in columns])), 256).alias(
            "_row_hash"
        ),
    )


def assert_historical_rows_unchanged(baseline: DataFrame, current: DataFrame) -> int:
    columns = baseline.columns
    baseline_hashes = hash_rows(baseline, columns).alias("baseline")
    current_hashes = hash_rows(current, columns).alias("current")
    mismatches = baseline_hashes.join(current_hashes, "trip_id").filter(
        F.col("baseline._row_hash") != F.col("current._row_hash")
    )
    mismatch_count = mismatches.count()
    if mismatch_count:
        raise ValueError(f"{mismatch_count} baseline integrated rows changed")
    return mismatch_count


def bounds(frame: DataFrame, columns: list[str]) -> dict[str, Any]:
    expressions = []
    for name in columns:
        expressions.extend(
            (
                F.min(name).alias(f"min_{name}"),
                F.max(name).alias(f"max_{name}"),
            )
        )
    return {
        key: str(value) if value is not None else None
        for key, value in frame.agg(*expressions).first().asDict().items()
    }


def context_statistics(integrated: DataFrame) -> dict[str, int]:
    expressions = []
    for endpoint in ("pickup", "dropoff"):
        expressions.extend(
            (
                F.sum(F.col(f"{endpoint}_weather_matched").cast("long")).alias(
                    f"{endpoint}_weather_matched"
                ),
                F.sum(
                    (F.col(f"{endpoint}_pm25_source") != "missing").cast("long")
                ).alias(f"{endpoint}_air_quality_matched"),
                F.sum(
                    (F.col(f"{endpoint}_pm25_source") == "missing").cast("long")
                ).alias(f"{endpoint}_air_quality_missing"),
                F.sum((F.col(f"{endpoint}_zone") == "Unknown").cast("long")).alias(
                    f"{endpoint}_unknown_zone"
                ),
                F.sum((F.col(f"{endpoint}_borough") == "Unknown").cast("long")).alias(
                    f"{endpoint}_unknown_borough"
                ),
            )
        )
    values = integrated.agg(*expressions).first().asDict()
    return {name: int(value or 0) for name, value in values.items()}


def source_window_summary(frame: DataFrame, timestamp_column: str) -> dict[str, Any]:
    result = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(timestamp_column).alias("distinct_timestamps"),
        F.min(timestamp_column).alias("min_timestamp"),
        F.max(timestamp_column).alias("max_timestamp"),
    ).first()
    return {
        "rows": int(result["rows"]),
        "distinct_timestamps": int(result["distinct_timestamps"]),
        "min_timestamp": str(result["min_timestamp"]),
        "max_timestamp": str(result["max_timestamp"]),
    }


def source_increment_summary(
    spark: SparkSession, path: str, key_column: str, timestamp_column: str
) -> dict[str, Any]:
    baseline_keys = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(path)
        .select(key_column)
        .distinct()
    )
    current = spark.read.format("delta").load(path)
    inserted = current.join(baseline_keys, key_column, "left_anti")
    return source_window_summary(inserted, timestamp_column)


def _run_incremental_integration(
    spark: SparkSession, config: dict[str, Any]
) -> dict[str, Any]:
    started = time.perf_counter()
    baseline_evidence = json.loads(BASELINE_EVIDENCE_PATH.read_text(encoding="utf-8"))
    integrated_config = baseline_evidence["integrated_dataset"]
    integrated_path = integrated_config["table"]
    pipeline = UrbanDataIntegrationPipeline.from_config(spark, config)
    sources = pipeline.load_sources()
    integrated_baseline = spark.read.format("delta").load(integrated_path).cache()

    version_before = current_version(spark, integrated_path)
    baseline_rows = table_count(integrated_baseline)
    if (
        version_before != EXPECTED_BASELINE_VERSION
        or baseline_rows != EXPECTED_BASELINE_ROWS
        or baseline_rows != integrated_config["row_count"]
    ):
        raise ValueError(
            "Integrated baseline does not match the frozen Week 2 version 0 state"
        )

    source_versions_before = {
        name: current_version(spark, path)
        for name, path in (
            ("taxi", pipeline.taxi_delta_path),
            ("weather", pipeline.weather_delta_path),
            ("air_quality", pipeline.air_quality_delta_path),
            ("zones", pipeline.zones_delta_path),
        )
    }
    if source_versions_before != EXPECTED_SOURCE_VERSIONS:
        raise ValueError(
            "Task 1 Silver source versions differ from expected state: "
            f"{source_versions_before}"
        )

    product_paths = {
        name: str(ROOT / "storage/delta/gold" / name) for name in GOLD_PRODUCTS
    }
    product_versions_before = {
        name: current_version(spark, path) for name, path in product_paths.items()
    }

    baseline_keys = integrated_baseline.select("trip_id").distinct()
    new_taxi = sources["taxi"].join(baseline_keys, "trip_id", "left_anti").persist()
    new_taxi_rows = table_count(new_taxi)
    taxi_identity = new_taxi.agg(
        F.countDistinct("trip_id").alias("distinct_ids"),
        F.sum(F.col("trip_id").isNull().cast("long")).alias("null_ids"),
    ).first()
    if (
        taxi_identity["distinct_ids"] != new_taxi_rows
        or taxi_identity["null_ids"]
        or new_taxi_rows == 0
    ):
        raise ValueError("Incremental Taxi candidates have invalid trip_id identity")

    built_integrated = pipeline.build_integrated_dataset(
        taxi_df=new_taxi,
        weather_df=sources["weather"],
        air_df=sources["air"],
        zones_df=sources["zones"],
    ).persist()
    integrated_rows = pipeline.assert_trip_identity(new_taxi, built_integrated)
    integrated_new = built_integrated.select(*integrated_baseline.columns).persist()
    built_integrated.unpersist()
    if integrated_rows != new_taxi_rows:
        raise ValueError("Integration changed candidate row count")

    target_schema = [
        (field.name, field.dataType.json())
        for field in integrated_baseline.schema.fields
    ]
    candidate_schema = [
        (field.name, field.dataType.json()) for field in integrated_new.schema.fields
    ]
    if candidate_schema != target_schema:
        raise ValueError("Incremental integrated rows do not match baseline schema")

    row_bounds = bounds(
        integrated_new,
        ["pickup_date", "pickup_local_datetime", "pickup_datetime"],
    )
    contexts = context_statistics(integrated_new)
    weather_scope = source_increment_summary(
        spark,
        pipeline.weather_delta_path,
        "weather_observation_id",
        "observation_datetime",
    )
    air_scope = source_increment_summary(
        spark,
        pipeline.air_quality_delta_path,
        "air_quality_observation_id",
        "observation_datetime",
    )

    premerge_duplicate_count = integrated_new.join(
        baseline_keys, "trip_id", "left_semi"
    ).count()
    if premerge_duplicate_count:
        raise ValueError("Incremental integrated candidates contain baseline trip IDs")

    delta_target = DeltaTable.forPath(spark, integrated_path)
    (
        delta_target.alias("target")
        .merge(
            integrated_new.alias("source"),
            "target.trip_id = source.trip_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    final_integrated = spark.read.format("delta").load(integrated_path)
    final_rows = table_count(final_integrated)
    version_after = current_version(spark, integrated_path)
    merge_history = (
        DeltaTable.forPath(spark, integrated_path)
        .history()
        .filter(F.col("operation") == "MERGE")
        .orderBy(F.desc("version"))
        .select("version", "operation")
        .first()
    )
    if final_rows != baseline_rows + new_taxi_rows:
        raise ValueError("Final integrated count does not equal baseline plus inserts")

    inserted_ids = integrated_new.select("trip_id")
    inserted_missing = inserted_ids.join(
        final_integrated.select("trip_id"), "trip_id", "left_anti"
    ).count()
    historical_mismatches = assert_historical_rows_unchanged(
        integrated_baseline, final_integrated
    )

    source_versions_after = {
        name: current_version(spark, path)
        for name, path in (
            ("taxi", pipeline.taxi_delta_path),
            ("weather", pipeline.weather_delta_path),
            ("air_quality", pipeline.air_quality_delta_path),
            ("zones", pipeline.zones_delta_path),
        )
    }
    product_versions_after = {
        name: current_version(spark, path) for name, path in product_paths.items()
    }
    if source_versions_after != source_versions_before:
        raise ValueError("Silver source table versions changed during integration")
    if product_versions_after != product_versions_before:
        raise ValueError("Week 2 Gold product versions changed during integration")
    if inserted_missing:
        raise ValueError(f"{inserted_missing} inserted trip IDs are absent after merge")

    baseline_schema = integrated_baseline.schema.jsonValue()
    final_schema = final_integrated.schema.jsonValue()
    if baseline_schema != final_schema:
        raise ValueError("Integrated table schema changed during insert-only merge")

    historical_date_max = integrated_config["max_pickup_date"]
    baseline_time_columns = [
        "pickup_local_datetime",
        "pickup_datetime",
        "pickup_date",
    ]
    baseline_time_bounds = bounds(integrated_baseline, baseline_time_columns)
    execution_seconds = round(time.perf_counter() - started, 3)
    evidence = {
        "baseline": {
            "source_evidence": str(BASELINE_EVIDENCE_PATH.relative_to(ROOT)),
            "table": integrated_path,
            "delta_version_before": version_before,
            "row_count": baseline_rows,
            "schema_version": integrated_config["schema_version"],
            "schema_version_unavailable_reason": integrated_config[
                "schema_version_unavailable_reason"
            ],
            "pickup_date_range": {
                "min": integrated_config["min_pickup_date"],
                "max": historical_date_max,
            },
            "pickup_timestamp_bounds": baseline_time_bounds,
            "partition_columns": integrated_config["partition_columns"],
        },
        "incremental_scope": {
            "business_key": "trip_id",
            "scope_method": "Left anti-join current clean Taxi Silver against integrated baseline trip_id.",
            "clean_taxi_rows": table_count(sources["taxi"]),
            "new_taxi_candidates": new_taxi_rows,
            "new_integrated_rows": integrated_rows,
            "pickup_bounds": row_bounds,
            "duplicate_trip_ids_ignored": premerge_duplicate_count,
            "rejected_or_unsupported_rows": 0,
        },
        "context_coverage": {
            "match_definition": {
                "weather": "Existing integration pipeline matched a containing UTC pickup/dropoff hour exactly.",
                "air_quality": "Existing integration pipeline used exact UTC hour and borough PM2.5 with citywide fallback.",
                "zones": "Existing integration pipeline left-joined pickup/dropoff location IDs to the zone lookup.",
            },
            **contexts,
            "task1_weather_window_source": weather_scope,
            "task1_air_quality_window_source": air_scope,
        },
        "schema_evolution": {
            "integrated_schema_version_before": integrated_config["schema_version"],
            "integrated_schema_version_after": integrated_config["schema_version"],
            "schema_version_unavailable_reason": integrated_config[
                "schema_version_unavailable_reason"
            ],
            "delta_schema_changes": [],
            "task1_humidity_propagated": False,
            "task1_aqi_propagated": False,
            "decision": "Keep humidity and aqi in their Silver source tables. Existing integration projects established relative_humidity_pct into *_humidity_pct and measurement-derived PM2.5 into *_pm25; Week 2 queries do not depend on the new fields. The integrated schema remains unchanged.",
        },
        "execution": {
            "operation": "Delta MERGE, insert-only on trip_id",
            "delta_merge_version": int(merge_history["version"]),
            "delta_version_before": version_before,
            "delta_version_after": version_after,
            "baseline_rows": baseline_rows,
            "inserted_rows": integrated_rows,
            "final_rows": final_rows,
            "execution_seconds": execution_seconds,
            "status": "success",
        },
        "correctness_checks": {
            "new_records_selected_by_trip_id_anti_join": True,
            "new_taxi_trip_ids_unique_and_non_null": True,
            "all_new_taxi_candidates_integrated": integrated_rows == new_taxi_rows,
            "final_count_equals_baseline_plus_inserted": (
                final_rows == baseline_rows + integrated_rows
            ),
            "inserted_ids_present_after_merge": inserted_missing == 0,
            "duplicate_rows_ignored": premerge_duplicate_count == 0,
            "baseline_rows_unchanged": historical_mismatches == 0,
            "no_duplicate_integrated_trip_ids": (
                final_integrated.select("trip_id").distinct().count() == final_rows
            ),
            "existing_join_semantics_reused": True,
            "source_silver_delta_versions_unchanged": (
                source_versions_after == source_versions_before
            ),
            "week2_gold_products_untouched": (
                product_versions_after == product_versions_before
            ),
            "integrated_schema_unchanged": baseline_schema == final_schema,
            "snapshot_overwrite_used": False,
            "delta_merge_present_in_history": merge_history is not None,
            "task1_raw_files_read_or_modified": False,
            "source_silver_versions_before": source_versions_before,
            "source_silver_versions_after": source_versions_after,
            "week2_gold_versions_before": product_versions_before,
            "week2_gold_versions_after": product_versions_after,
        },
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")

    integrated_new.unpersist()
    new_taxi.unpersist()
    integrated_baseline.unpersist()
    return evidence


def run(spark: SparkSession, config: dict[str, Any]) -> dict[str, Any]:
    """Run the unchanged integration refresh with operational monitoring."""
    monitoring = PipelineMonitor(
        spark,
        "week3_task2_incremental_integration",
        "integration",
        "integrated_taxi_trips",
    ).start_execution(
        input_path=config["datasets"]["taxi_trips"]["silver_path"],
        target_path=config["integration"]["output_path"],
    )
    try:
        evidence = _run_incremental_integration(spark, config)
        execution = evidence["execution"]
        scope = evidence["incremental_scope"]
        schema = evidence["schema_evolution"]
        monitoring.finalize_success(
            execution,
            processed_records=scope["new_taxi_candidates"],
            updated_records=0,
            duplicate_records=scope["duplicate_trip_ids_ignored"],
            rejected_records=scope["rejected_or_unsupported_rows"],
            schema_version_before=schema["integrated_schema_version_before"],
            schema_version_after=schema["integrated_schema_version_after"],
            schema_changes=json.dumps(schema["delta_schema_changes"]),
            execution_seconds=execution["execution_seconds"],
            delta_version_before=execution["delta_version_before"],
            delta_version_after=execution["delta_version_after"],
        )
        return evidence
    except Exception as error:
        monitoring.finalize_failure(error)
        raise


def main() -> None:
    config = load_config(ROOT / "config/platform_config.yaml")
    spark = get_spark_session(**config["spark"])
    try:
        print(json.dumps(run(spark, config), indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
