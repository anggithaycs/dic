"""Apply the Week 3 Weather update to the existing Silver Delta table."""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from src.common.config import load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.weather_ingestion import WeatherIngestor
from src.monitoring.pipeline_monitor import PipelineMonitor


UPDATE_PATH = "data/updates/weather_update.csv"
TARGET_PATH = "storage/delta/silver/clean_weather"
METADATA_PATH = "storage/delta/metadata/incremental_runs"
RELEASE_START = "2025-01-01 01:00:00"
RELEASE_END = "2025-01-08 00:00:00"


def build_update_ingestor(spark: SparkSession, config: dict) -> WeatherIngestor:
    spec = copy.deepcopy(config["datasets"]["weather"])
    spec["source_path"] = UPDATE_PATH
    spec["casts"]["humidity"] = "double"
    spec["quality_rules"]["valid_year"] = 2025
    spec["quality_rules"]["humidity_min"] = 20.0
    spec["quality_rules"]["humidity_max"] = 100.0
    spec["validation"]["valid_update_window"] = (
        "observation_datetime >= timestamp('2025-01-01 01:00:00') AND "
        "observation_datetime <= timestamp('2025-01-08 00:00:00')"
    )
    spec["validation"]["valid_new_humidity"] = (
        "humidity BETWEEN {humidity_min} AND {humidity_max}"
    )
    spec["selected_columns"] = [*spec["selected_columns"], "humidity"]
    return WeatherIngestor(
        spark=spark,
        dataset_spec=spec,
        schema_version=config["platform"]["version"],
        metadata_path=METADATA_PATH,
    )


def record_metadata(spark: SparkSession, metrics: dict) -> None:
    (
        spark.createDataFrame([metrics])
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(METADATA_PATH)
    )


def run(spark: SparkSession, config: dict) -> dict:
    start = time.perf_counter()
    run_id = uuid4().hex
    monitoring = PipelineMonitor(
        spark, "week3_weather_incremental", "incremental_ingestion", "weather"
    ).start_execution(run_id, input_path=UPDATE_PATH, target_path=TARGET_PATH)
    monitoring_metrics: dict = {
        "schema_version_before": config["platform"]["version"],
    }
    try:
        ingestor = build_update_ingestor(spark, config)
        target = spark.read.format("delta").load(TARGET_PATH)
        baseline_count = target.count()
        schema_before = target.schema.simpleString()
        monitoring_metrics["delta_version_before"] = int(
            DeltaTable.forPath(spark, TARGET_PATH).history(1).first()["version"]
        )
        if "humidity" in target.columns:
            raise ValueError("Weather target already contains humidity; refusing to rerun")

        raw = ingestor.load_raw_data().persist()
        try:
            input_records = raw.count()
            monitoring_metrics["input_records"] = input_records
            prepared = ingestor.prepare(raw)
            ingestor.validate_schema_compatibility(prepared)
            target_keys = target.select("weather_observation_id").distinct()
            duplicate_rows = prepared.join(
                target_keys, "weather_observation_id", "left_semi"
            ).persist()
            new_candidates = prepared.join(
                target_keys, "weather_observation_id", "left_anti"
            ).persist()
            try:
                duplicate_records = duplicate_rows.count()
                monitoring_metrics["duplicate_records"] = duplicate_records
                valid, rejected = ingestor.apply_data_quality_rules(new_candidates)
                rejected_records = rejected.count()
                monitoring_metrics["rejected_records"] = rejected_records
                monitoring_metrics["validation_failure_count"] = rejected_records
                if rejected_records:
                    ingestor.persist_rejected_records(rejected, run_id=run_id)
                insert_source = valid.dropDuplicates(
                    ["weather_observation_id"]
                ).persist()
                try:
                    inserted_records = insert_source.count()
                    monitoring_metrics["inserted_records"] = inserted_records
                    if set(insert_source.columns) != {
                        *target.columns,
                        "humidity",
                    }:
                        raise ValueError(
                            "Validated Weather update schema does not match target plus humidity"
                        )

                    delta_target = DeltaTable.forPath(spark, TARGET_PATH)
                    (
                        delta_target.alias("target")
                        .merge(
                            insert_source.alias("source"),
                            "target.weather_observation_id = "
                            "source.weather_observation_id",
                        )
                        .withSchemaEvolution()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                finally:
                    insert_source.unpersist()
            finally:
                duplicate_rows.unpersist()
                new_candidates.unpersist()
        finally:
            raw.unpersist()

        final_target = spark.read.format("delta").load(TARGET_PATH)
        final_count = final_target.count()
        humidity_populated = (
            final_target.filter(
                F.col("observation_datetime").between(
                    F.to_timestamp(F.lit(RELEASE_START)),
                    F.to_timestamp(F.lit(RELEASE_END)),
                )
                & F.col("humidity").isNotNull()
            ).count()
            == inserted_records
        )
        duration = round(time.perf_counter() - start, 3)
        result = {
            "run_id": run_id,
            "dataset": "weather",
            "input_path": UPDATE_PATH,
            "target_path": TARGET_PATH,
            "input_records": input_records,
            "inserted_records": inserted_records,
            "duplicate_records": duplicate_records,
            "updated_records": 0,
            "rejected_records": rejected_records,
            "baseline_count": baseline_count,
            "final_count": final_count,
            "schema_version_before": config["platform"]["version"],
            "schema_version_after": "2.1.0",
            "schema_before": schema_before,
            "schema_after": final_target.schema.simpleString(),
            "schema_changes": "+humidity",
            "humidity_exists": "humidity" in final_target.columns,
            "humidity_populated_for_inserted": humidity_populated,
            "execution_seconds": duration,
            "status": "success",
        }
        record_metadata(spark, result)
        monitoring.finalize_success(
            result,
            validation_failure_count=rejected_records,
            delta_version_before=monitoring_metrics["delta_version_before"],
            delta_version_after=int(
                DeltaTable.forPath(spark, TARGET_PATH).history(1).first()["version"]
            ),
        )
        return result
    except Exception as error:
        monitoring.finalize_failure(error, monitoring_metrics)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(ROOT / "config/platform_config.yaml"),
        help="Path to the platform configuration",
    )
    args = parser.parse_args()
    config = load_config(args.config)
    spark = get_spark_session(**config["spark"])
    try:
        print(json.dumps(run(spark, config), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
