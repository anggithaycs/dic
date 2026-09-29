"""Incrementally refresh the four Week 2 Gold products for Task 2."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.monitoring.pipeline_monitor import PipelineMonitor

ROOT = Path(__file__).resolve().parents[1]
BASELINE_EVIDENCE = ROOT / "storage/metrics/week3/task2_baseline.json"
INTEGRATION_EVIDENCE = ROOT / "storage/metrics/week3/task2_integration_refresh.json"
OUTPUT_EVIDENCE = ROOT / "storage/metrics/week3/product_comparison.json"
INTEGRATED_PATH = "storage/delta/silver/integrated_taxi_trips"
GOLD_ROOT = ROOT / "storage/delta/gold"
PRODUCT_NAMES = (
    "daily_mobility_summary",
    "taxi_zone_statistics",
    "weather_impact_summary",
    "air_quality_impact_summary",
)
EXPECTED_BASELINE_VERSION = 0
EXPECTED_UPDATED_INTEGRATED_VERSION = 1
FLOAT_TOLERANCE = 1e-8


def week2_sql(relative_path: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"week2:src/week2/sql/{relative_path}"],
        cwd=ROOT,
        text=True,
    )


def current_version(spark: SparkSession, path: str) -> int:
    return int(
        DeltaTable.forPath(spark, path)
        .history(1)
        .select("version")
        .first()["version"]
    )


def assert_same_schema(left: DataFrame, right: DataFrame, product: str) -> None:
    left_schema = [(f.name, f.dataType.json()) for f in left.schema.fields]
    right_schema = [(f.name, f.dataType.json()) for f in right.schema.fields]
    if left_schema != right_schema:
        raise ValueError(f"{product}: candidate schema differs from baseline")


def compare_to_reference(
    actual: DataFrame, reference: DataFrame, keys: list[str], product: str
) -> dict[str, int]:
    assert_same_schema(actual, reference, product)
    duplicate_actual = (
        actual.groupBy(*keys).count().filter(F.col("count") != 1).count()
    )
    duplicate_reference = (
        reference.groupBy(*keys).count().filter(F.col("count") != 1).count()
    )
    if duplicate_actual or duplicate_reference:
        raise ValueError(f"{product}: duplicate output keys detected")

    actual_keys = actual.select(*keys)
    reference_keys = reference.select(*keys)
    missing = reference_keys.join(actual_keys, keys, "left_anti").count()
    unexpected = actual_keys.join(reference_keys, keys, "left_anti").count()

    left = actual.alias("actual")
    right = reference.alias("reference")
    joined = left.join(right, keys, "full")
    value_differences = []
    for field in actual.schema.fields:
        name = field.name
        if name in keys:
            continue
        actual_value = F.col(f"actual.`{name}`")
        expected_value = F.col(f"reference.`{name}`")
        if field.dataType.simpleString() in {"double", "float"}:
            differs = (
                actual_value.isNull() != expected_value.isNull()
            ) | (
                actual_value.isNotNull()
                & expected_value.isNotNull()
                & (
                    F.abs(actual_value - expected_value)
                    > F.lit(FLOAT_TOLERANCE)
                    + F.lit(FLOAT_TOLERANCE)
                    * F.greatest(F.abs(actual_value), F.abs(expected_value))
                )
            )
        else:
            differs = ~actual_value.eqNullSafe(expected_value)
        value_differences.append(differs)
    value_mismatch_expr = value_differences[0]
    for expression in value_differences[1:]:
        value_mismatch_expr = value_mismatch_expr | expression
    value_mismatches = joined.filter(value_mismatch_expr).count()

    result = {
        "missing_reference_keys": int(missing),
        "unexpected_keys": int(unexpected),
        "value_mismatches": int(value_mismatches),
    }
    if any(result.values()):
        raise ValueError(f"{product}: incremental output differs from full reference: {result}")
    return result


def stage_incremental_hours(
    spark: SparkSession,
    new_trips: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    calendar_sql = week2_sql("views/calendar_hours.sql").format(
        start=start_date, end=end_date
    )
    spark.sql(calendar_sql).createOrReplaceTempView("incremental_calendar_hours")
    new_trips.createOrReplaceTempView("incremental_new_trips")
    counts = (
        new_trips.groupBy("hour_utc")
        .agg(
            F.count("*").alias("trip_count"),
            F.max("pickup_pm25_citywide").alias("pm25"),
            F.max("weather").alias("weather"),
        )
        .alias("counts")
    )
    calendar = spark.table("incremental_calendar_hours").alias("calendar")
    return calendar.join(
        counts,
        F.col("calendar.hour_utc") == F.col("counts.hour_utc"),
        "left",
    ).select(
        F.col("calendar.hour_utc").alias("hour_utc"),
        F.col("calendar.local_date").alias("local_date"),
        F.col("calendar.weekday").alias("weekday"),
        F.col("calendar.local_hour").alias("local_hour"),
        F.coalesce(F.col("counts.trip_count"), F.lit(0).cast("long")).alias(
            "trip_count"
        ),
        F.col("counts.pm25").alias("pm25"),
        F.coalesce(F.col("counts.weather"), F.lit("unknown")).alias("weather"),
    )


def append_merge(
    spark: SparkSession, path: str, source: DataFrame, condition: str
) -> None:
    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(source.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _run_incremental_products(spark: SparkSession) -> dict[str, Any]:
    baseline = json.loads(BASELINE_EVIDENCE.read_text(encoding="utf-8"))
    integration = json.loads(INTEGRATION_EVIDENCE.read_text(encoding="utf-8"))
    integrated = spark.read.format("delta").load(INTEGRATED_PATH).cache()
    integrated_version = current_version(spark, INTEGRATED_PATH)
    if integrated_version != EXPECTED_UPDATED_INTEGRATED_VERSION:
        raise ValueError(f"Expected integrated Delta version 1, found {integrated_version}")
    if int(integrated.count()) != int(integration["execution"]["final_rows"]):
        raise ValueError("Integrated table row count does not match Step 3 evidence")

    product_baselines: dict[str, DataFrame] = {}
    product_paths = {
        name: str(GOLD_ROOT / name) for name in PRODUCT_NAMES
    }
    product_versions = {
        name: current_version(spark, product_paths[name]) for name in PRODUCT_NAMES
    }
    for name in PRODUCT_NAMES:
        expected = baseline["products"]["items"][name]
        if product_versions[name] != expected["delta_version"]:
            raise ValueError(f"{name}: Gold baseline Delta version changed")
        product_baselines[name] = (
            spark.read.format("delta").load(product_paths[name]).cache()
        )
        if product_baselines[name].count() != expected["row_count"]:
            raise ValueError(f"{name}: Gold baseline row count changed")

    # Build the standard Week 2 temporary views and the full-SQL references in memory.
    integrated.createOrReplaceTempView("integrated_trips")
    spark.sql(week2_sql("views/trips.sql")).createOrReplaceTempView("trips")
    coverage = spark.sql(week2_sql("views/coverage.sql")).first().asDict()
    calendar_sql = week2_sql("views/calendar_hours.sql").format(
        start=coverage["first_date"], end=coverage["last_date"]
    )
    spark.sql(calendar_sql).createOrReplaceTempView("calendar_hours")
    for view_name in ("hourly_demand", "zone_weather"):
        spark.sql(week2_sql(f"views/{view_name}.sql")).createOrReplaceTempView(
            view_name
        )

    product_sql = {
        "daily_mobility_summary": week2_sql(
            "products/daily_mobility_summary.sql"
        ),
        "taxi_zone_statistics": week2_sql("products/taxi_zone_statistics.sql"),
        "weather_impact_summary": "SELECT * FROM zone_weather",
        "air_quality_impact_summary": "SELECT * FROM hourly_demand",
    }
    full_references: dict[str, DataFrame] = {}
    full_reference_seconds: dict[str, float] = {}
    for name, sql in product_sql.items():
        started = time.perf_counter()
        reference = spark.sql(sql).cache()
        reference.count()
        full_reference_seconds[name] = round(time.perf_counter() - started, 3)
        full_references[name] = reference

    baseline_integrated_path = INTEGRATED_PATH
    old_integrated = (
        spark.read.format("delta")
        .option(
            "versionAsOf",
            baseline["integrated_dataset"]["delta_version"],
        )
        .load(baseline_integrated_path)
        .cache()
    )
    new_integrated = integrated.join(
        old_integrated.select("trip_id"), "trip_id", "left_anti"
    ).cache()
    new_integrated_count = int(new_integrated.count())
    if new_integrated_count != int(integration["incremental_scope"]["new_integrated_rows"]):
        raise ValueError("New integrated Taxi scope does not match Step 3 evidence")

    new_integrated.createOrReplaceTempView("incremental_integrated_trips")
    new_trips_sql = week2_sql("views/trips.sql").replace(
        "FROM integrated_trips", "FROM incremental_integrated_trips"
    )
    spark.sql(new_trips_sql).createOrReplaceTempView("new_trips")
    new_trips = spark.table("new_trips").cache()
    new_trips.count()

    candidate_sources: dict[str, DataFrame] = {}
    incremental_seconds: dict[str, float] = {}
    affected_scopes: dict[str, dict[str, Any]] = {}
    keys_by_product = {
        "daily_mobility_summary": ["pickup_date", "pickup_borough"],
        "taxi_zone_statistics": ["month", "pickup_location_id"],
        "weather_impact_summary": [
            "pickup_location_id",
            "pickup_zone",
            "weather",
        ],
        "air_quality_impact_summary": ["hour_utc"],
    }

    # These groups occur only after the frozen January-March baseline.
    for name in ("daily_mobility_summary", "taxi_zone_statistics"):
        started = time.perf_counter()
        sql = product_sql[name].replace("FROM trips", "FROM new_trips")
        delta = spark.sql(sql).cache()
        delta.count()
        keys = keys_by_product[name]
        conflicts = delta.join(
            product_baselines[name].select(*keys), keys, "left_semi"
        ).count()
        if conflicts:
            raise ValueError(f"{name}: new groups overlap the frozen baseline")
        candidate = product_baselines[name].unionByName(delta).cache()
        candidate.count()
        affected_scopes[name] = {
            "strategy": "incremental",
            "scope": {
                "new_trip_rows": new_integrated_count,
                "minimum_pickup_date": integration["incremental_scope"][
                    "pickup_bounds"
                ]["min_pickup_date"],
                "maximum_pickup_date": integration["incremental_scope"][
                    "pickup_bounds"
                ]["max_pickup_date"],
                "new_aggregate_rows": int(delta.count()),
                "overlapping_baseline_groups": int(conflicts),
            },
        }
        candidate_sources[name] = delta
        incremental_seconds[name] = round(time.perf_counter() - started, 3)

    # Recompute hourly rows only after the end of the frozen integrated baseline.
    air_started = time.perf_counter()
    first_incremental_date = (
        spark.sql(
            "SELECT date_add(DATE '{}', 1) AS start_date".format(
                baseline["integrated_dataset"]["max_pickup_date"]
            )
        )
        .first()["start_date"]
        .isoformat()
    )
    updated_end_date = str(coverage["last_date"])
    incremental_hours = stage_incremental_hours(
        spark, new_trips, first_incremental_date, updated_end_date
    ).cache()
    incremental_hours.count()
    air_candidate = incremental_hours.cache()
    air_candidate.count()
    air_overlap = air_candidate.join(
        product_baselines["air_quality_impact_summary"].select("hour_utc"),
        "hour_utc",
        "left_semi",
    ).count()
    if air_overlap:
        raise ValueError("Air Quality product: incremental calendar overlaps baseline")
    candidate_sources["air_quality_impact_summary"] = air_candidate
    air_combined = (
        product_baselines["air_quality_impact_summary"]
        .unionByName(air_candidate)
        .cache()
    )
    air_combined.count()
    incremental_seconds["air_quality_impact_summary"] = round(
        time.perf_counter() - air_started, 3
    )
    affected_scopes["air_quality_impact_summary"] = {
        "strategy": "affected-partition",
        "scope": {
            "hour_utc_after_baseline": True,
            "calendar_start_local_date": first_incremental_date,
            "calendar_end_local_date": updated_end_date,
            "incremental_hour_rows": int(air_candidate.count()),
            "overlapping_baseline_hours": int(air_overlap),
        },
    }

    # Weather impact includes exposure hours by category, shared by every zone.
    weather_started = time.perf_counter()
    base_weather = product_baselines["weather_impact_summary"]
    new_weather_totals = (
        new_trips.groupBy("pickup_location_id", "pickup_zone", "weather")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("trip_distance").alias("distance_sum"),
            F.count("trip_distance").alias("distance_count"),
        )
        .cache()
    )
    new_weather_totals.count()
    new_exposure = incremental_hours.groupBy("weather").agg(
        F.count("*").alias("added_observed_hours")
    )
    base_exposure = base_weather.groupBy("weather").agg(
        F.min("observed_hours").alias("baseline_observed_hours"),
        F.max("observed_hours").alias("maximum_baseline_observed_hours"),
    )
    inconsistent_exposure = base_exposure.filter(
        F.col("baseline_observed_hours") != F.col("maximum_baseline_observed_hours")
    ).count()
    if inconsistent_exposure:
        raise ValueError("Baseline Weather Impact has inconsistent exposure by zone")

    categories = (
        base_exposure.select("weather")
        .unionByName(new_exposure.select("weather"))
        .distinct()
    )
    zones = (
        spark.table("trips")
        .select("pickup_location_id", "pickup_zone")
        .distinct()
    )
    group_keys = zones.crossJoin(categories)
    base_values = base_weather.select(
        "pickup_location_id",
        "pickup_zone",
        "weather",
        F.col("observed_hours").alias("baseline_observed_hours"),
        F.col("trip_count").alias("baseline_trip_count"),
        F.col("distance_sum").alias("baseline_distance_sum"),
        F.col("distance_count").alias("baseline_distance_count"),
    )
    new_totals = new_weather_totals.select(
        "pickup_location_id",
        "pickup_zone",
        "weather",
        F.col("trip_count").alias("added_trip_count"),
        F.col("distance_sum").alias("added_distance_sum"),
        F.col("distance_count").alias("added_distance_count"),
    )
    weather_candidate = (
        group_keys.join(
            base_values,
            ["pickup_location_id", "pickup_zone", "weather"],
            "left",
        )
        .join(
            new_totals,
            ["pickup_location_id", "pickup_zone", "weather"],
            "left",
        )
        .join(
            base_exposure.select(
                "weather",
                F.col("baseline_observed_hours").alias("prior_exposure_hours"),
            ),
            ["weather"],
            "left",
        )
        .join(new_exposure, ["weather"], "left")
        .select(
            "pickup_location_id",
            "pickup_zone",
            "weather",
            (
                F.coalesce(
                    F.col("prior_exposure_hours"),
                    F.lit(0).cast("long"),
                )
                + F.coalesce(F.col("added_observed_hours"), F.lit(0).cast("long"))
            ).alias("observed_hours"),
            (
                F.coalesce(F.col("baseline_trip_count"), F.lit(0).cast("long"))
                + F.coalesce(F.col("added_trip_count"), F.lit(0).cast("long"))
            ).alias("trip_count"),
            (
                F.coalesce(F.col("baseline_distance_sum"), F.lit(0.0))
                + F.coalesce(F.col("added_distance_sum"), F.lit(0.0))
            ).alias("distance_sum"),
            (
                F.coalesce(F.col("baseline_distance_count"), F.lit(0).cast("long"))
                + F.coalesce(F.col("added_distance_count"), F.lit(0).cast("long"))
            ).alias("distance_count"),
        )
        .cache()
    )
    weather_candidate.count()
    candidate_sources["weather_impact_summary"] = weather_candidate
    incremental_seconds["weather_impact_summary"] = round(
        time.perf_counter() - weather_started, 3
    )
    affected_scopes["weather_impact_summary"] = {
        "strategy": "affected-partition",
        "scope": {
            "updated_zone_weather_groups": int(weather_candidate.count()),
            "new_zone_weather_trip_groups": int(new_weather_totals.count()),
            "weather_categories": [
                row["weather"] for row in categories.orderBy("weather").collect()
            ],
            "exposure_hours_recomputed_for_extension": True,
            "extension_calendar_start_local_date": first_incremental_date,
            "extension_calendar_end_local_date": updated_end_date,
        },
    }

    reference_names = {
        "daily_mobility_summary": ["pickup_date", "pickup_borough"],
        "taxi_zone_statistics": ["month", "pickup_location_id"],
        "weather_impact_summary": [
            "pickup_location_id",
            "pickup_zone",
            "weather",
        ],
        "air_quality_impact_summary": ["hour_utc"],
    }
    candidate_full: dict[str, DataFrame] = {
        "daily_mobility_summary": (
            product_baselines["daily_mobility_summary"]
            .unionByName(candidate_sources["daily_mobility_summary"])
            .cache()
        ),
        "taxi_zone_statistics": (
            product_baselines["taxi_zone_statistics"]
            .unionByName(candidate_sources["taxi_zone_statistics"])
            .cache()
        ),
        "weather_impact_summary": candidate_sources["weather_impact_summary"],
        "air_quality_impact_summary": air_combined,
    }
    equivalence = {}
    for name in PRODUCT_NAMES:
        candidate_full[name].count()
        equivalence[name] = compare_to_reference(
            candidate_full[name],
            full_references[name],
            reference_names[name],
            name,
        )

    versions_before = dict(product_versions)
    for name in ("daily_mobility_summary", "taxi_zone_statistics"):
        merge_started = time.perf_counter()
        append_merge(
            spark,
            product_paths[name],
            candidate_sources[name],
            " AND ".join(
                f"target.`{key}` = source.`{key}`" for key in reference_names[name]
            ),
        )
        incremental_seconds[name] = round(
            incremental_seconds[name] + time.perf_counter() - merge_started, 3
        )
    merge_started = time.perf_counter()
    append_merge(
        spark,
        product_paths["air_quality_impact_summary"],
        candidate_sources["air_quality_impact_summary"],
        "target.hour_utc = source.hour_utc",
    )
    incremental_seconds["air_quality_impact_summary"] = round(
        incremental_seconds["air_quality_impact_summary"]
        + time.perf_counter()
        - merge_started,
        3,
    )
    merge_started = time.perf_counter()
    (
        DeltaTable.forPath(spark, product_paths["weather_impact_summary"])
        .alias("target")
        .merge(
            candidate_sources["weather_impact_summary"].alias("source"),
            "target.pickup_location_id = source.pickup_location_id "
            "AND target.pickup_zone = source.pickup_zone "
            "AND target.weather = source.weather",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    incremental_seconds["weather_impact_summary"] = round(
        incremental_seconds["weather_impact_summary"]
        + time.perf_counter()
        - merge_started,
        3,
    )

    products_evidence = {}
    for name in PRODUCT_NAMES:
        path = product_paths[name]
        final_product = spark.read.format("delta").load(path)
        final_count = int(final_product.count())
        final_version = current_version(spark, path)
        final_equivalence = compare_to_reference(
            final_product, full_references[name], reference_names[name], name
        )
        metadata = baseline["products"]["items"][name]
        before_schema = metadata["schema"]
        after_schema = [
            {"name": field.name, "type": field.dataType.typeName()}
            for field in final_product.schema.fields
        ]
        products_evidence[name] = {
            "product_name": metadata["product_name"],
            "affected": True,
            "classification": (
                "A"
                if name in ("daily_mobility_summary", "taxi_zone_statistics")
                else "B"
            ),
            "refresh_strategy": affected_scopes[name]["strategy"],
            "reason": {
                "daily_mobility_summary": "Additive count/sum/non-null-count aggregate groups are exclusively for dates after the baseline.",
                "taxi_zone_statistics": "Additive count/sum/non-null-count aggregate groups are exclusively for months after the baseline.",
                "weather_impact_summary": "New trip totals are additive, while observed_hours changes by weather category for every zone; only the small zone-weather product rows are recomposed from the baseline aggregates and new-scope aggregates/exposures.",
                "air_quality_impact_summary": "The product is hourly and appendable after the baseline calendar end; hours are recomputed only for the newly covered calendar interval.",
            }[name],
            "baseline_row_count": int(metadata["row_count"]),
            "updated_row_count": final_count,
            "affected_scope": affected_scopes[name]["scope"],
            "schema_before": before_schema,
            "schema_after": after_schema,
            "schema_compatible": before_schema == after_schema,
            "incremental_refresh_time_seconds": incremental_seconds[name],
            "full_refresh_reference_time_seconds": full_reference_seconds[name],
            "output_equivalent_to_full_refresh": final_equivalence == {
                "missing_reference_keys": 0,
                "unexpected_keys": 0,
                "value_mismatches": 0,
            },
            "equivalence_check": final_equivalence,
            "baseline_delta_version": versions_before[name],
            "updated_delta_version": final_version,
            "schema_evolution_fields_used": [],
            "notes": {
                "weather_impact_summary": "New humidity is not used; established precipitation-derived weather categories remain the grouping field.",
                "air_quality_impact_summary": "New aqi is not used; established PM2.5 context remains in this hourly product.",
            }.get(name, "New humidity and aqi are not used by this product."),
        }

    evidence = {
        "baseline_run_id": baseline["products"]["baseline_run_id"],
        "integrated_delta_version": integrated_version,
        "products": products_evidence,
        "all_products_equivalent_to_full_refresh": all(
            item["output_equivalent_to_full_refresh"]
            for item in products_evidence.values()
        ),
        "gold_refresh_mode": "Incremental/affected-scope Delta MERGE; no product snapshot overwrite.",
        "final_product_benchmark_performed": False,
    }
    OUTPUT_EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_EVIDENCE.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")

    for frame in (
        integrated,
        old_integrated,
        new_integrated,
        new_trips,
        incremental_hours,
        air_combined,
        new_weather_totals,
        weather_candidate,
        *product_baselines.values(),
        *full_references.values(),
        *candidate_full.values(),
    ):
        frame.unpersist()
    return evidence


def run(spark: SparkSession) -> dict[str, Any]:
    """Run unchanged Gold refreshes with one operational record per product."""
    monitoring = {
        name: PipelineMonitor(
            spark,
            "week3_task2_incremental_products",
            "gold_refresh",
            name,
        ).start_execution(
            input_path=INTEGRATED_PATH,
            target_path=str(GOLD_ROOT / name),
        )
        for name in PRODUCT_NAMES
    }
    processed_scope_fields = {
        "daily_mobility_summary": "new_aggregate_rows",
        "taxi_zone_statistics": "new_aggregate_rows",
        "weather_impact_summary": "updated_zone_weather_groups",
        "air_quality_impact_summary": "incremental_hour_rows",
    }
    insert_only_products = {
        "daily_mobility_summary",
        "taxi_zone_statistics",
        "air_quality_impact_summary",
    }
    try:
        evidence = _run_incremental_products(spark)
        for name in PRODUCT_NAMES:
            product = evidence["products"][name]
            processed_records = product["affected_scope"][
                processed_scope_fields[name]
            ]
            finalization_fields: dict[str, Any] = {
                "processed_records": processed_records,
                "execution_seconds": product["incremental_refresh_time_seconds"],
                "delta_version_before": product["baseline_delta_version"],
                "delta_version_after": product["updated_delta_version"],
                "schema_changes": json.dumps(product["schema_evolution_fields_used"]),
            }
            if name in insert_only_products:
                finalization_fields["inserted_records"] = processed_records
                finalization_fields["updated_records"] = 0
            monitoring[name].finalize_success(**finalization_fields)
        return evidence
    except Exception as error:
        for execution in monitoring.values():
            execution.finalize_failure(error)
        raise


def main() -> None:
    from src.common.config import load_config
    from src.common.spark_session import get_spark_session, stop_spark_session

    config = load_config(ROOT / "config/platform_config.yaml")
    spark = get_spark_session(**config["spark"])
    try:
        print(json.dumps(run(spark), indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
