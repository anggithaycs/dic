"""Focused tests for train-fitted Week 4 Spark feature engineering."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from src.common.spark_session import get_spark_session, stop_spark_session
from src.week4.dataset import load_week4_config
from src.week4.features import (
    fit_transform_feature_dataset,
    load_feature_dataset,
    materialize_feature_dataset,
)


class Week4FeatureTests(unittest.TestCase):
    """Check chronology-safe fitting, null handling, and feature-vector output."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_spark_session(
            app_name="Week4FeatureTests", master="local[2]", shuffle_partitions=2
        )
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tempdir.name)
        cls.source_path = str(cls.root / "hourly_demand")
        cls._write_source()

    @classmethod
    def tearDownClass(cls) -> None:
        stop_spark_session(cls.spark)
        cls.tempdir.cleanup()

    @classmethod
    def _write_source(cls) -> None:
        rows = []
        for index, temperature in enumerate((10.0, 20.0, 30.0), start=1):
            rows.append(cls._row(
                zone=index,
                borough="TrainBorough",
                split="train",
                target=5.0 + index,
                temperature=temperature,
                weather_code=1,
                pm_source="borough",
                target_hour=datetime(2024, 1, 1) + timedelta(hours=index),
            ))
        # Validation deliberately has categories unseen in train and a null
        # numeric value. Train's temperature median is exactly 20.0.
        rows.append(cls._row(
            zone=999,
            borough="UnseenBorough",
            split="validation",
            target=42.0,
            temperature=None,
            weather_code=99,
            pm_source="new-source",
            target_hour=datetime(2024, 2, 1),
        ))
        rows.append(cls._row(
            zone=998,
            borough="TestBorough",
            split="test",
            target=43.0,
            temperature=None,
            weather_code=98,
            pm_source="test-source",
            target_hour=datetime(2024, 3, 1),
        ))
        schema = StructType([
            StructField("pickup_location_id", IntegerType(), False),
            StructField("pickup_borough", StringType(), False),
            StructField("decision_hour", TimestampType(), False),
            StructField("target_hour", TimestampType(), False),
            StructField("next_hour_demand", DoubleType(), False),
            StructField("split", StringType(), False),
            StructField("hour", IntegerType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("is_weekend", BooleanType(), False),
            StructField("demand_lag_1h", DoubleType(), False),
            StructField("demand_lag_24h", DoubleType(), False),
            StructField("rolling_mean_24h", DoubleType(), False),
            StructField("prior_hour_pickup_temperature_c", DoubleType(), True),
            StructField("prior_hour_pickup_temperature_c_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_humidity_pct", DoubleType(), False),
            StructField("prior_hour_pickup_humidity_pct_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_precipitation_mm", DoubleType(), True),
            StructField("prior_hour_pickup_precipitation_mm_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_weather_code", IntegerType(), False),
            StructField("prior_hour_pickup_weather_code_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_wind_speed_kmh", DoubleType(), False),
            StructField("prior_hour_pickup_wind_speed_kmh_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_pm25", DoubleType(), False),
            StructField("prior_hour_pickup_pm25_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_weather_matched", BooleanType(), False),
            StructField("prior_hour_pickup_weather_matched_is_missing", BooleanType(), False),
            StructField("prior_hour_pickup_pm25_source", StringType(), False),
            StructField("prior_hour_pickup_pm25_source_is_missing", BooleanType(), False),
            StructField("trip_id", StringType(), False),
            StructField("fare_amount", DoubleType(), False),
        ])
        cls.spark.createDataFrame(rows, schema).write.format("delta").save(cls.source_path)

    @staticmethod
    def _row(
        zone: int,
        borough: str,
        split: str,
        target: float,
        temperature: float | None,
        weather_code: int,
        pm_source: str,
        target_hour: datetime,
    ) -> dict:
        return {
            "pickup_location_id": zone,
            "pickup_borough": borough,
            "decision_hour": target_hour - timedelta(hours=1),
            "target_hour": target_hour,
            "next_hour_demand": target,
            "split": split,
            "hour": target_hour.hour,
            "day_of_week": 2,
            "month": target_hour.month,
            "is_weekend": False,
            "demand_lag_1h": 4.0,
            "demand_lag_24h": 3.0,
            "rolling_mean_24h": 3.5,
            "prior_hour_pickup_temperature_c": temperature,
            "prior_hour_pickup_temperature_c_is_missing": temperature is None,
            "prior_hour_pickup_humidity_pct": 55.0,
            "prior_hour_pickup_humidity_pct_is_missing": False,
            "prior_hour_pickup_precipitation_mm": 0.0,
            "prior_hour_pickup_precipitation_mm_is_missing": False,
            "prior_hour_pickup_weather_code": weather_code,
            "prior_hour_pickup_weather_code_is_missing": False,
            "prior_hour_pickup_wind_speed_kmh": 7.0,
            "prior_hour_pickup_wind_speed_kmh_is_missing": False,
            "prior_hour_pickup_pm25": 9.0,
            "prior_hour_pickup_pm25_is_missing": False,
            "prior_hour_pickup_weather_matched": True,
            "prior_hour_pickup_weather_matched_is_missing": False,
            "prior_hour_pickup_pm25_source": pm_source,
            "prior_hour_pickup_pm25_source_is_missing": False,
            "trip_id": "excluded-leakage-field",
            "fare_amount": 99.0,
        }

    def config(self) -> dict:
        config = copy.deepcopy(load_week4_config())
        spec = config["feature_pipeline"]
        spec["input_delta_path"] = self.source_path
        spec["output_delta_path"] = str(self.root / "features")
        spec["evidence_path"] = str(self.root / "task2.json")
        return config

    def transformed(self):
        return fit_transform_feature_dataset(
            load_feature_dataset(self.spark, self.config()), self.config()
        )

    def test_fit_uses_train_statistics_only_and_handles_numeric_nulls(self) -> None:
        result = self.transformed()
        validation = result.transformed.filter(F.col("split") == "validation").first()
        self.assertEqual(validation["__imputed_prior_hour_pickup_temperature_c"], 20.0)
        self.assertIsNotNone(validation["features"])

    def test_all_null_train_numeric_feature_fails_with_actionable_name(self) -> None:
        dataset = load_feature_dataset(self.spark, self.config()).withColumn(
            "prior_hour_pickup_temperature_c",
            F.when(F.col("split") == "train", F.lit(None).cast("double")).otherwise(
                F.col("prior_hour_pickup_temperature_c")
            ),
        )
        with self.assertRaisesRegex(
            ValueError, "prior_hour_pickup_temperature_c"
        ):
            fit_transform_feature_dataset(dataset, self.config())

    def test_unseen_categories_preserve_rows_labels_and_splits(self) -> None:
        result = self.transformed()
        output = result.model_ready
        self.assertEqual(output.count(), 5)
        self.assertEqual(
            {row["split"] for row in output.select("split").distinct().collect()},
            {"train", "validation", "test"},
        )
        validation = output.filter(F.col("split") == "validation").first()
        self.assertEqual(validation.label, 42.0)
        self.assertIsNotNone(validation.features)

    def test_model_output_has_no_leakage_and_is_deterministic(self) -> None:
        first = self.transformed().model_ready
        second = self.transformed().model_ready
        self.assertEqual(first.schema, second.schema)
        self.assertEqual(first.count(), second.count())
        first_size = first.select(F.size(vector_to_array("features"))).first()[0]
        second_size = second.select(F.size(vector_to_array("features"))).first()[0]
        self.assertEqual(first_size, second_size)
        self.assertNotIn("decision_hour", first.columns)
        self.assertNotIn("trip_id", first.columns)
        self.assertNotIn("fare_amount", first.columns)

    def test_materialization_writes_model_ready_output_and_evidence(self) -> None:
        evidence = materialize_feature_dataset(self.spark, self.config())
        output = self.spark.read.format("delta").load(
            self.config()["feature_pipeline"]["output_delta_path"]
        )
        self.assertEqual(output.count(), 5)
        self.assertEqual(evidence["split_rows"], {"train": 3, "validation": 1, "test": 1})
        saved = json.loads(Path(self.config()["feature_pipeline"]["evidence_path"]).read_text())
        self.assertTrue(saved["fitted_on_train_only"])
        self.assertEqual(saved["null_counts_after_transformation"]["features"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
