"""Task 4 raw-versus-platform comparison for the common Week 4 ML contract."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping

from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import build_ingestors, load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.week4.dataset import (
    DEFAULT_CONFIG_PATH,
    build_hourly_demand_dataset_from_source,
    load_week4_config,
)
from src.week4.evaluation import regression_metrics
from src.week4.features import fit_transform_feature_dataset
from src.week4.training import build_candidate_regressors


ROOT = Path(__file__).resolve().parents[2]


def _comparison_spec(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Validate the common-snapshot comparison contract."""
    spec = config.get("platform_comparison")
    if not isinstance(spec, Mapping):
        raise ValueError("Week 4 config: platform_comparison is required for Task 4")
    required = {
        "platform_integrated_path",
        "platform_delta_version",
        "raw_platform_config_path",
        "evidence_path",
        "floating_point_tolerance",
    }
    missing = sorted(required - set(spec))
    if missing:
        raise ValueError(f"Week 4 platform_comparison: missing {missing}")
    if spec["platform_delta_version"] != 0:
        raise ValueError("Task 4 requires platform_delta_version = 0 for the common baseline snapshot")
    return spec


def production_delta_versions(delta_root: Path | None = None) -> dict[str, int]:
    """Read Delta-log versions for production tables without opening them for write."""
    default_root = ROOT / "storage" / "delta"
    root = delta_root or default_root
    versions: dict[str, int] = {}
    for log_dir in root.rglob("_delta_log"):
        table = log_dir.parent
        # Week 4 ML outputs are not production platform tables and are not
        # touched by this comparison either.
        if table.is_relative_to(root / "ml"):
            continue
        log_versions = [
            int(path.stem)
            for path in log_dir.glob("*.json")
            if path.stem.isdigit()
        ]
        if log_versions:
            relative_table = table.relative_to(root)
            key = (
                str(Path("storage/delta") / relative_table)
                if root == default_root
                else str(relative_table)
            )
            versions[key] = max(log_versions)
    return dict(sorted(versions.items()))


def comparison_metadata(config: Mapping[str, Any]) -> dict[str, Any]:
    """Return deterministic, measurable workflow-complexity metadata."""
    spec = _comparison_spec(config)
    raw_config = load_config(ROOT / spec["raw_platform_config_path"])
    return {
        "common_prediction_contract": "Task 1 next-hour pickup-zone demand contract",
        "production_delta_writes": False,
        "artifact_write": spec["evidence_path"],
        "approach_a_raw": {
            "source_snapshot": {
                "definition": "Week 1 raw Jan--Mar 2024 files declared in platform_config.yaml",
                "paths": {
                    name: raw_config["datasets"][name]["source_path"]
                    for name in ("taxi_trips", "weather", "air_quality", "taxi_zones")
                },
            },
            "source_datasets_read": 4,
            "source_dataset_names": ["taxi_trips", "weather", "air_quality", "taxi_zones"],
            "validation_normalization_stages_per_dataset": [
                "load_raw_data", "standardize_column_names/casts", "filter_scope",
                "normalize_and_transform", "classify_records/split_records",
            ],
            "integration_join_count": 8,
            "major_transformations_before_week4_features": 6,
            "reused_components": [
                "BaseDatasetIngestor.prepare",
                "BaseDatasetIngestor.apply_data_quality_rules",
                "UrbanDataIntegrationPipeline.build_integrated_dataset",
                "build_hourly_demand_dataset_from_source",
                "fit_transform_feature_dataset",
            ],
        },
        "approach_b_platform": {
            "source_snapshot": {
                "path": spec["platform_integrated_path"],
                "version_as_of": spec["platform_delta_version"],
            },
            "source_datasets_read": 1,
            "source_dataset_names": ["integrated_taxi_trips"],
            "validation_normalization_stages_per_dataset": [],
            "integration_join_count": 0,
            "major_transformations_before_week4_features": 1,
            "reused_components": [
                "Delta versionAsOf reader",
                "build_hourly_demand_dataset_from_source",
                "fit_transform_feature_dataset",
            ],
        },
    }


def _timed_count(frame: DataFrame) -> tuple[DataFrame, float, int]:
    """Materialize a DataFrame once and return its elapsed wall-clock time."""
    start = time.perf_counter()
    cached = frame.cache()
    count = cached.count()
    return cached, time.perf_counter() - start, count


def _build_raw_integrated(
    spark: SparkSession, config: Mapping[str, Any]
) -> tuple[DataFrame, dict[str, Any], list[DataFrame]]:
    """Reuse Week 1 ingestion and integration in memory without any Delta writes."""
    comparison = _comparison_spec(config)
    platform_config = load_config(ROOT / comparison["raw_platform_config_path"])
    ingestors = build_ingestors(spark, platform_config)
    ordered_names = ["taxi_zones", "weather", "air_quality", "taxi_trips"]
    raw_frames: dict[str, DataFrame] = {}
    prepared_frames: dict[str, DataFrame] = {}
    valid_frames: dict[str, DataFrame] = {}
    retained: list[DataFrame] = []
    timings: dict[str, float] = {}
    counts: dict[str, dict[str, int]] = {}

    start = time.perf_counter()
    for name in ordered_names:
        frame, _, count = _timed_count(ingestors[name].load_raw_data())
        raw_frames[name] = frame
        retained.append(frame)
        counts[name] = {"raw_rows": count}
    timings["raw_source_loading_seconds"] = time.perf_counter() - start

    start = time.perf_counter()
    for name in ordered_names:
        frame, _, count = _timed_count(ingestors[name].prepare(raw_frames[name]))
        # The source snapshot is the version-0 baseline, which predates the
        # later Task 4 batch-schema contract. Keep the original row-level
        # validation path below rather than rejecting historical extra columns.
        prepared_frames[name] = frame
        retained.append(frame)
        counts[name]["prepared_rows"] = count
    timings["cleaning_normalization_seconds"] = time.perf_counter() - start

    start = time.perf_counter()
    for name in ordered_names:
        # Version 0 predates Task 4's reference-check extension. The original
        # baseline contract used cast, SQL-rule, key, and duplicate validation.
        valid, _ = ingestors[name].apply_data_quality_rules(
            prepared_frames[name], include_reference_checks=False
        )
        frame, _, count = _timed_count(valid)
        valid_frames[name] = frame
        retained.append(frame)
        counts[name]["validated_rows"] = count
    timings["validation_seconds"] = time.perf_counter() - start

    pipeline = UrbanDataIntegrationPipeline(
        spark=spark,
        taxi_delta_path="",
        weather_delta_path="",
        air_quality_delta_path="",
        zones_delta_path="",
        output_delta_path="",
    )
    start = time.perf_counter()
    integrated, _, integrated_rows = _timed_count(pipeline.build_integrated_dataset(
        valid_frames["taxi_trips"],
        valid_frames["weather"],
        valid_frames["air_quality"],
        valid_frames["taxi_zones"],
    ))
    retained.append(integrated)
    timings["integration_seconds"] = time.perf_counter() - start
    counts["integrated"] = {"rows": integrated_rows}
    return integrated, {"runtime": timings, "source_dataset_counts": counts}, retained


def _build_platform_integrated(
    spark: SparkSession, config: Mapping[str, Any]
) -> tuple[DataFrame, dict[str, Any], list[DataFrame]]:
    """Load exactly the required integrated Delta version without a production write."""
    spec = _comparison_spec(config)
    start = time.perf_counter()
    frame, _, count = _timed_count(
        spark.read.format("delta")
        .option("versionAsOf", spec["platform_delta_version"])
        .load(spec["platform_integrated_path"])
    )
    return frame, {
        "runtime": {"integrated_delta_loading_seconds": time.perf_counter() - start},
        "source_dataset_counts": {"integrated": {"rows": count}},
    }, [frame]


def _build_ml_and_features(
    integrated: DataFrame, config: Mapping[str, Any]
) -> tuple[DataFrame, Any, dict[str, float], list[DataFrame]]:
    """Apply the same Task 1 and Task 2 contracts to either integrated source."""
    timings: dict[str, float] = {}
    start = time.perf_counter()
    ml_dataset, _, _ = _timed_count(build_hourly_demand_dataset_from_source(integrated, config))
    timings["ml_dataset_construction_seconds"] = time.perf_counter() - start
    start = time.perf_counter()
    transformation = fit_transform_feature_dataset(ml_dataset, config)
    features, _, _ = _timed_count(transformation.model_ready)
    timings["feature_preprocessing_seconds"] = time.perf_counter() - start
    return ml_dataset, transformation, timings, [ml_dataset, features]


def _split_counts(frame: DataFrame) -> dict[str, int]:
    return {row["split"]: row["count"] for row in frame.groupBy("split").count().collect()}


def _key_stats(frame: DataFrame) -> dict[str, int]:
    return {
        "rows": frame.count(),
        "distinct_pickup_zone_target_hour_keys": frame.select(
            "pickup_location_id", "target_hour"
        ).distinct().count(),
    }


def _compare_columns(
    left: DataFrame,
    right: DataFrame,
    columns: list[str],
    tolerance: float,
) -> dict[str, Any]:
    """Compare keyed ML rows with exact or tolerance-aware per-column checks."""
    keys = ["pickup_location_id", "target_hour"]
    left_alias = left.select(*keys, *columns).alias("left")
    right_alias = right.select(*keys, *columns).alias("right")
    joined = left_alias.join(right_alias, keys, "full_outer")
    checks = []
    maximums = []
    left_schema = {field.name: field.dataType.simpleString() for field in left.schema.fields}
    for column in columns:
        lcol, rcol = F.col(f"left.{column}"), F.col(f"right.{column}")
        if left_schema[column] in {"double", "float"}:
            mismatch = (~lcol.isNull()) & (~rcol.isNull()) & (F.abs(lcol - rcol) > tolerance)
            mismatch = mismatch | (lcol.isNull() != rcol.isNull())
            maximums.append(
                F.max(
                    F.when(
                        lcol.isNotNull() & rcol.isNotNull(), F.abs(lcol - rcol)
                    ).otherwise(F.lit(0.0))
                ).alias(f"{column}__max_abs_difference")
            )
        else:
            mismatch = ~lcol.eqNullSafe(rcol)
        checks.append(F.sum(mismatch.cast("long")).alias(column))
    result = joined.agg(*checks, *maximums).first().asDict()
    mismatches = {column: result[column] for column in columns}
    maximum_differences = {
        column: result.get(f"{column}__max_abs_difference", 0.0) or 0.0
        for column in columns
        if f"{column}__max_abs_difference" in result
    }
    return {
        "mismatched_rows_by_column": mismatches,
        "max_abs_difference_by_numeric_column": maximum_differences,
        "passed": all(value == 0 for value in mismatches.values()),
    }


def compare_ml_inputs(
    raw_ml: DataFrame,
    platform_ml: DataFrame,
    raw_features: DataFrame,
    platform_features: DataFrame,
    tolerance: float,
) -> dict[str, Any]:
    """Verify Task 1 rows and Task 2 vectors are equivalent before model fitting."""
    ml_columns = [
        "pickup_borough", "split", "next_hour_demand", "demand_lag_1h",
        "demand_lag_24h", "rolling_mean_24h", "prior_hour_pickup_temperature_c",
        "prior_hour_pickup_humidity_pct", "prior_hour_pickup_precipitation_mm",
        "prior_hour_pickup_weather_code", "prior_hour_pickup_wind_speed_kmh",
        "prior_hour_pickup_pm25", "prior_hour_pickup_weather_matched",
        "prior_hour_pickup_pm25_source",
    ]
    row_stats = {"raw": _key_stats(raw_ml), "platform": _key_stats(platform_ml)}
    split_stats = {"raw": _split_counts(raw_ml), "platform": _split_counts(platform_ml)}
    zero_shares = {
        "raw": raw_ml.agg(F.avg((F.col("next_hour_demand") == 0).cast("double"))).first()[0],
        "platform": platform_ml.agg(F.avg((F.col("next_hour_demand") == 0).cast("double"))).first()[0],
    }
    ml_equivalence = _compare_columns(raw_ml, platform_ml, ml_columns, tolerance)
    feature_dimension = {
        "raw": raw_features.select(F.size(vector_to_array("features"))).first()[0],
        "platform": platform_features.select(F.size(vector_to_array("features"))).first()[0],
    }
    keys = ["pickup_location_id", "target_hour"]
    vectors = raw_features.select(*keys, "label", vector_to_array("features").alias("raw_features")).alias("raw").join(
        platform_features.select(*keys, "label", vector_to_array("features").alias("platform_features")).alias("platform"),
        keys,
        "full_outer",
    )
    max_difference = vectors.select(
        F.max(F.array_max(F.transform(
            F.arrays_zip("raw_features", "platform_features"),
            lambda value: F.abs(value["raw_features"] - value["platform_features"]),
        ))).alias("max_feature_abs_difference"),
        F.sum((~F.col("raw.label").eqNullSafe(F.col("platform.label"))).cast("long")).alias("label_mismatches"),
    ).first().asDict()
    feature_equivalence = {
        "feature_dimension": feature_dimension,
        **max_difference,
        "passed": feature_dimension["raw"] == feature_dimension["platform"]
        and (max_difference["max_feature_abs_difference"] or 0.0) <= tolerance
        and max_difference["label_mismatches"] == 0,
    }
    passed = (
        row_stats["raw"] == row_stats["platform"]
        and split_stats["raw"] == split_stats["platform"]
        and zero_shares["raw"] == zero_shares["platform"]
        and ml_equivalence["passed"]
        and feature_equivalence["passed"]
    )
    return {
        "passed": passed,
        "row_key_stats": row_stats,
        "split_row_counts": split_stats,
        "zero_demand_target_share": zero_shares,
        "ml_column_equivalence": ml_equivalence,
        "feature_equivalence": feature_equivalence,
        "floating_point_tolerance": tolerance,
    }


def _fit_linear_regression(
    transformation: Any, config: Mapping[str, Any]
) -> tuple[float, dict[str, float]]:
    """Fit the fixed Task 3 linear-regression configuration and score validation."""
    train = transformation.model_ready.filter(F.col("split") == "train").cache()
    validation = transformation.model_ready.filter(F.col("split") == "validation").cache()
    try:
        start = time.perf_counter()
        model = build_candidate_regressors(config)["linear_regression"].fit(train)
        fit_seconds = time.perf_counter() - start
        return fit_seconds, regression_metrics(model.transform(validation))
    finally:
        train.unpersist()
        validation.unpersist()


def run_platform_comparison(spark: SparkSession, config: Mapping[str, Any]) -> dict[str, Any]:
    """Run equivalence first, then compare raw and platform LinearRegression fits."""
    spec = _comparison_spec(config)
    retained: list[DataFrame] = []
    production_versions_before = production_delta_versions()
    try:
        raw_integrated, raw_info, raw_frames = _build_raw_integrated(spark, config)
        retained.extend(raw_frames)
        platform_integrated, platform_info, platform_frames = _build_platform_integrated(spark, config)
        retained.extend(platform_frames)
        raw_ml, raw_transformation, raw_times, raw_ml_frames = _build_ml_and_features(raw_integrated, config)
        platform_ml, platform_transformation, platform_times, platform_ml_frames = _build_ml_and_features(platform_integrated, config)
        retained.extend(raw_ml_frames + platform_ml_frames)
        equivalence = compare_ml_inputs(
            raw_ml, platform_ml, raw_transformation.model_ready,
            platform_transformation.model_ready, float(spec["floating_point_tolerance"]),
        )
        if not equivalence["passed"]:
            raise ValueError(f"Task 4 common-output equivalence failed: {equivalence}")
        raw_fit_seconds, raw_validation_metrics = _fit_linear_regression(
            raw_transformation, config
        )
        platform_fit_seconds, platform_validation_metrics = _fit_linear_regression(
            platform_transformation, config
        )
        production_versions_after = production_delta_versions()
        if production_versions_after != production_versions_before:
            raise RuntimeError(
                "Task 4 comparison unexpectedly changed a production Delta table: "
                f"before={production_versions_before}, after={production_versions_after}"
            )
        raw_runtime = {**raw_info["runtime"], **raw_times, "linear_regression_fit_seconds": raw_fit_seconds}
        platform_runtime = {
            **platform_info["runtime"],
            **platform_times,
            "linear_regression_fit_seconds": platform_fit_seconds,
        }
        raw_runtime["total_source_to_trained_model_seconds"] = sum(raw_runtime.values())
        platform_runtime["total_source_to_trained_model_seconds"] = sum(platform_runtime.values())
        metadata = comparison_metadata(config)
        evidence = {
            "source_snapshot_definitions": {
                "approach_a_raw": metadata["approach_a_raw"]["source_snapshot"],
                "approach_b_platform": metadata["approach_b_platform"]["source_snapshot"],
            },
            "approach_a_stages": metadata["approach_a_raw"],
            "approach_b_stages": metadata["approach_b_platform"],
            "output_equivalence": equivalence,
            "source_dataset_counts": {
                "approach_a_raw": raw_info["source_dataset_counts"],
                "approach_b_platform": platform_info["source_dataset_counts"],
            },
            "runtime_breakdown_seconds": {
                "approach_a_raw": raw_runtime,
                "approach_b_platform": platform_runtime,
            },
            "linear_regression_configuration": config["model_training"]["candidates"]["linear_regression"],
            "linear_regression_validation_metrics": {
                "approach_a_raw": raw_validation_metrics,
                "approach_b_platform": platform_validation_metrics,
            },
            "reproducibility": {
                "approach_a_raw": {
                    "raw_files": config["platform_comparison"]["raw_platform_config_path"],
                    "required_processing": "Existing Week 1 preparation, validation, deduplication, and integration configuration",
                    "integration_dependencies": ["taxi zones", "weather", "air quality"],
                },
                "approach_b_platform": {
                    "pinned_delta_version": 0,
                    "integrated_schema_source": config["platform_comparison"]["platform_integrated_path"],
                    "required_processing": "Task 1 dataset construction and Task 2 feature preprocessing only",
                },
            },
            "production_delta_writes": metadata["production_delta_writes"],
            "production_delta_version_check": {
                "passed": True,
                "before": production_versions_before,
                "after": production_versions_after,
            },
            "limitations": [
                "Single local-run timings are descriptive and may include Spark cache/JVM noise.",
                "Approach A uses the current reusable ingestion code with reference checks disabled to reproduce the original version-0 baseline validation contract.",
                "Approach A also bypasses batch-schema compatibility because that contract was introduced after the version-0 source snapshot.",
            ],
            "benchmark_environment": {
                "spark_master": "local[2]",
                "feature_vector_size": equivalence["feature_equivalence"]["feature_dimension"]["raw"],
            },
        }
        evidence_path = Path(spec["evidence_path"])
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
        return evidence
    finally:
        for frame in retained:
            frame.unpersist()


def main() -> None:
    """Execute the isolated Task 4 platform comparison."""
    config = load_week4_config(DEFAULT_CONFIG_PATH)
    spark = get_spark_session(app_name="Week4PlatformComparison", master="local[2]", shuffle_partitions=8)
    try:
        print(json.dumps(run_platform_comparison(spark, config), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
