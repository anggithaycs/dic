"""Build a causal, zone-hour Taxi demand dataset from integrated Delta data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.spark_session import get_spark_session, stop_spark_session

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT / "config/week4.yaml"


def load_week4_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load and minimally validate the Week 4 dataset contract."""
    with Path(path).open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    required_sections = {
        "source",
        "output",
        "target",
        "lags",
        "splits",
        "environmental_features",
        "leakage_columns",
    }
    missing = sorted(required_sections - set(config or {}))
    if missing:
        raise ValueError(f"Week 4 config: missing sections {missing}")

    source = config["source"]
    for name in (
        "delta_path",
        "delta_version",
        "decision_timestamp_column",
        "pickup_zone_column",
        "pickup_borough_column",
    ):
        if name not in source:
            raise ValueError(f"Week 4 source: missing {name}")
    if not isinstance(source["delta_version"], int) or source["delta_version"] < 0:
        raise ValueError("Week 4 source.delta_version must be a non-negative integer")

    splits = config["splits"]
    if set(splits) != {"train", "validation", "test"}:
        raise ValueError("Week 4 splits must define train, validation, and test")
    boundaries = []
    for name in ("train", "validation", "test"):
        split = splits[name]
        if not {"start", "end"} <= set(split):
            raise ValueError(f"Week 4 split {name}: start and end are required")
        if split["start"] > split["end"]:
            raise ValueError(f"Week 4 split {name}: start must not follow end")
        boundaries.append((split["start"], split["end"]))
    if boundaries[0][1] >= boundaries[1][0] or boundaries[1][1] >= boundaries[2][0]:
        raise ValueError("Week 4 splits must be chronological and non-overlapping")
    return config


def _required_source_columns(config: Mapping[str, Any]) -> set[str]:
    source = config["source"]
    return {
        source["decision_timestamp_column"],
        source["pickup_zone_column"],
        source["pickup_borough_column"],
        *config["environmental_features"],
    }


def _load_source(spark: SparkSession, config: Mapping[str, Any]) -> DataFrame:
    """Read the explicitly pinned integrated Delta snapshot."""
    source = config["source"]
    frame = (
        spark.read.format("delta")
        .option("versionAsOf", source["delta_version"])
        .load(source["delta_path"])
    )
    missing = sorted(_required_source_columns(config) - set(frame.columns))
    if missing:
        raise ValueError(f"Integrated Delta snapshot is missing required columns: {missing}")
    return frame


def _hourly_context(
    source: DataFrame, config: Mapping[str, Any]
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Create demand plus weather-by-hour and PM2.5-by-borough-hour context."""
    source_spec = config["source"]
    zone = source_spec["pickup_zone_column"]
    borough = source_spec["pickup_borough_column"]
    decision_timestamp = source_spec["decision_timestamp_column"]
    environmental = config["environmental_features"]

    selected = source.select(
        F.col(zone).alias("pickup_location_id"),
        F.coalesce(F.col(borough), F.lit("Unknown")).alias("pickup_borough"),
        F.date_trunc("hour", F.col(decision_timestamp)).alias("decision_hour"),
        *[F.col(column) for column in environmental],
    ).filter(
        F.col("pickup_location_id").isNotNull()
        & F.col("decision_hour").isNotNull()
    )

    zones = selected.groupBy("pickup_location_id").agg(
        F.first("pickup_borough", ignorenulls=True).alias("pickup_borough")
    )
    demand = selected.groupBy("pickup_location_id", "decision_hour").agg(
        F.count("*").cast("long").alias("observed_demand")
    )

    weather_columns = [
        column
        for column in environmental
        if column not in {"pickup_pm25", "pickup_pm25_source"}
    ]
    pm_columns = ["pickup_pm25", "pickup_pm25_source"]

    def aggregations_for(columns: list[str]) -> list:
        aggregations = []
        for column in columns:
            if column == "pickup_weather_matched":
                aggregations.append(
                    F.max(F.col(column).cast("int")).cast("boolean").alias(column)
                )
            else:
                aggregations.append(
                    F.first(F.col(column), ignorenulls=True).alias(column)
                )
        return aggregations

    weather_context = selected.groupBy("decision_hour").agg(
        *aggregations_for(weather_columns)
    )
    pm_context = selected.groupBy("pickup_borough", "decision_hour").agg(
        *aggregations_for(pm_columns)
    )
    return zones, demand, weather_context, pm_context


def _attach_context(
    grid: DataFrame, weather_context: DataFrame, pm_context: DataFrame
) -> DataFrame:
    """Attach context at its natural grain before prior-hour shifting."""
    return (
        grid.join(weather_context, "decision_hour", "left")
        .join(pm_context, ["pickup_borough", "decision_hour"], "left")
    )


def _complete_grid(zones: DataFrame, demand: DataFrame) -> DataFrame:
    """Generate all zone-hour rows between the observed first and last hour."""
    bounds = demand.agg(
        F.min("decision_hour").alias("first_hour"),
        F.max("decision_hour").alias("last_hour"),
    ).first()
    if bounds.first_hour is None or bounds.last_hour is None:
        raise ValueError("Integrated Delta snapshot contains no usable pickup timestamps")
    hours = zones.sparkSession.range(1).select(
        F.explode(
            F.sequence(
                F.lit(bounds.first_hour),
                F.lit(bounds.last_hour),
                F.expr("INTERVAL 1 HOUR"),
            )
        ).alias("decision_hour")
    )
    return (
        zones.crossJoin(hours)
        .join(demand, ["pickup_location_id", "decision_hour"], "left")
        .withColumn("demand", F.coalesce(F.col("observed_demand"), F.lit(0)).cast("long"))
        .drop("observed_demand")
    )


def _add_causal_features(
    grid: DataFrame,
    weather_context: DataFrame,
    pm_context: DataFrame,
    config: Mapping[str, Any],
) -> DataFrame:
    """Create target-relative causal features for next-hour demand forecasting.

    A row's ``decision_hour`` is one hour before its ``target_hour``.  Therefore
    demand and environmental context at ``decision_hour`` are available at
    prediction time and are exactly one hour before the target.
    """
    environmental = config["environmental_features"]
    horizon = int(config["target"]["horizon_hours"])
    include_rolling = bool(config["lags"]["include_rolling_mean_24h"])
    window = Window.partitionBy("pickup_location_id").orderBy("decision_hour")
    # decision_hour = target_hour - 1h.  This frame consequently contains
    # demand from target_hour - 24h through target_hour - 1h, inclusive.
    target_prior_24 = window.rowsBetween(-23, 0)

    frame = _attach_context(grid, weather_context, pm_context)
    frame = frame.withColumn(
        "target_hour", F.expr(f"decision_hour + INTERVAL {horizon} HOUR")
    )
    frame = frame.withColumn(
        config["target"]["name"], F.lead("demand", horizon).over(window)
    )
    # The current decision hour is target_hour - 1h.  Use it directly for the
    # one-hour lag, and look back 23 grid rows for target_hour - 24h.
    frame = frame.withColumn("demand_lag_1h", F.col("demand"))
    frame = frame.withColumn("demand_lag_24h", F.lag("demand", 23).over(window))
    if include_rolling:
        frame = frame.withColumn(
            "_rolling_count", F.count("demand").over(target_prior_24)
        )
        frame = frame.withColumn(
            "rolling_mean_24h",
            F.when(
                F.col("_rolling_count") == 24,
                F.avg("demand").over(target_prior_24),
            ),
        ).drop("_rolling_count")

    for column in environmental:
        prior = f"prior_hour_{column}"
        # Context is joined at decision_hour, which is target_hour - 1h.
        frame = frame.withColumn(prior, F.col(column))
        frame = frame.withColumn(f"{prior}_is_missing", F.col(prior).isNull())

    # Calendar characteristics of the forecast hour are known in advance.
    return (
        frame.withColumn("hour", F.hour("target_hour"))
        .withColumn("day_of_week", F.dayofweek("target_hour"))
        .withColumn("month", F.month("target_hour"))
        .withColumn("is_weekend", F.dayofweek("target_hour").isin(1, 7))
    )


def _assign_split(frame: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    """Assign split from target time so every feature precedes its label."""
    splits = config["splits"]
    return frame.withColumn(
        "split",
        F.when(
            F.to_date("target_hour").between(
                splits["train"]["start"], splits["train"]["end"]
            ),
            F.lit("train"),
        )
        .when(
            F.to_date("target_hour").between(
                splits["validation"]["start"], splits["validation"]["end"]
            ),
            F.lit("validation"),
        )
        .when(
            F.to_date("target_hour").between(
                splits["test"]["start"], splits["test"]["end"]
            ),
            F.lit("test"),
        ),
    )


def build_hourly_demand_dataset(
    spark: SparkSession, config: Mapping[str, Any]
) -> DataFrame:
    """Return the leakage-safe next-hour demand dataset without writing it."""
    return build_hourly_demand_dataset_from_source(_load_source(spark, config), config)


def build_hourly_demand_dataset_from_source(
    source: DataFrame, config: Mapping[str, Any]
) -> DataFrame:
    """Apply the unchanged Task 1 contract to an already-integrated DataFrame."""
    zones, demand, weather_context, pm_context = _hourly_context(source, config)
    frame = _add_causal_features(
        _complete_grid(zones, demand), weather_context, pm_context, config
    )
    target = config["target"]["name"]
    frame = _assign_split(frame, config).filter(
        F.col(target).isNotNull() & F.col("split").isNotNull()
    )
    if config["lags"]["drop_rows_without_lag_24h"]:
        frame = frame.filter(F.col("demand_lag_24h").isNotNull())
        if config["lags"]["include_rolling_mean_24h"]:
            frame = frame.filter(F.col("rolling_mean_24h").isNotNull())

    environmental = config["environmental_features"]
    columns = [
        "pickup_location_id",
        "pickup_borough",
        "decision_hour",
        "target_hour",
        target,
        "split",
        "hour",
        "day_of_week",
        "month",
        "is_weekend",
        "demand_lag_1h",
        "demand_lag_24h",
    ]
    if config["lags"]["include_rolling_mean_24h"]:
        columns.append("rolling_mean_24h")
    for column in environmental:
        columns.extend((f"prior_hour_{column}", f"prior_hour_{column}_is_missing"))
    return frame.select(*columns)


def _dataset_evidence(dataset: DataFrame, config: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize a materialized training dataset without fitting any estimator."""
    target = config["target"]["name"]
    total_rows = dataset.count()
    split_rows = {
        row["split"]: row["count"] for row in dataset.groupBy("split").count().collect()
    }
    target_stats = dataset.agg(
        F.min(target).alias("minimum"),
        F.max(target).alias("maximum"),
        F.avg(target).alias("mean"),
        F.expr(f"percentile_approx({target}, 0.5)").alias("median"),
        F.avg((F.col(target) == 0).cast("double")).alias("zero_share"),
    ).first().asDict()
    candidate_columns = [
        column
        for column in dataset.columns
        if column.startswith("prior_hour_") and not column.endswith("_is_missing")
    ]
    missingness = {
        column: value / total_rows
        for column, value in dataset.agg(
            *[
                F.sum(F.col(column).isNull().cast("long")).alias(column)
                for column in candidate_columns
            ]
        )
        .first()
        .asDict()
        .items()
    }
    lag_availability = dataset.agg(
        F.avg(F.col("demand_lag_1h").isNotNull().cast("double")).alias(
            "lag_1h_available_rate"
        ),
        F.avg(F.col("demand_lag_24h").isNotNull().cast("double")).alias(
            "lag_24h_available_rate"
        ),
    ).first().asDict()
    if config["lags"]["include_rolling_mean_24h"]:
        lag_availability["rolling_mean_24h_available_rate"] = dataset.agg(
            F.avg(F.col("rolling_mean_24h").isNotNull().cast("double"))
        ).first()[0]
    coverage = dataset.agg(
        F.min("decision_hour").cast("string").alias("first_decision_hour"),
        F.max("target_hour").cast("string").alias("last_target_hour"),
        F.countDistinct("pickup_location_id").alias("zone_count"),
    ).first().asDict()
    return {
        "source_delta_path": config["source"]["delta_path"],
        "source_delta_version": config["source"]["delta_version"],
        "row_grain": "pickup_location_id x local target hour",
        "target_definition": (
            f"trip count in the same pickup zone "
            f"{config['target']['horizon_hours']} hour ahead"
        ),
        "forecast_horizon_hours": config["target"]["horizon_hours"],
        "date_coverage": coverage,
        "total_rows": total_rows,
        "split_rows": split_rows,
        "zero_demand_target_share": target_stats.pop("zero_share"),
        "target_summary_statistics": target_stats,
        "feature_list": [
            column
            for column in dataset.columns
            if column not in {target, "split", "target_hour"}
        ],
        "excluded_leakage_columns": config["leakage_columns"],
        "missingness_per_candidate_feature": missingness,
        "lag_availability": lag_availability,
        "split_boundaries": config["splits"],
    }


def materialize_training_dataset(
    spark: SparkSession, config: Mapping[str, Any]
) -> dict[str, Any]:
    """Write only the dedicated Week 4 ML Delta dataset and JSON evidence."""
    dataset = build_hourly_demand_dataset(spark, config).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        evidence = _dataset_evidence(dataset, config)
        output_path = Path(config["output"]["delta_path"])
        (
            dataset.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(output_path))
        )
        evidence_path = Path(config["output"]["evidence_path"])
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(
            json.dumps(evidence, indent=2, default=str), encoding="utf-8"
        )
        return evidence
    finally:
        dataset.unpersist()


def main() -> None:
    """Materialize the approved version-0 Week 4 baseline dataset."""
    config = load_week4_config()
    spark = get_spark_session(
        app_name="Week4HourlyDemandDataset", master="local[2]", shuffle_partitions=8
    )
    try:
        print(json.dumps(materialize_training_dataset(spark, config), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
