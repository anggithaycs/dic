"""Focused contract tests for the Week 4 raw-versus-platform comparison."""

from __future__ import annotations

import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.spark_session import get_spark_session, stop_spark_session
from src.week4.comparison import (
    _comparison_spec,
    compare_ml_inputs,
    comparison_metadata,
    production_delta_versions,
)
from src.week4.dataset import load_week4_config


class Week4PlatformComparisonTests(unittest.TestCase):
    """Ensure the comparison gate is deterministic and contract-focused."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_spark_session(
            app_name="Week4PlatformComparisonTests", master="local[2]", shuffle_partitions=2
        )

    @classmethod
    def tearDownClass(cls) -> None:
        stop_spark_session(cls.spark)

    def config(self) -> dict:
        return copy.deepcopy(load_week4_config())

    @staticmethod
    def _ml_schema() -> StructType:
        return StructType([
            StructField("pickup_location_id", IntegerType(), False),
            StructField("target_hour", TimestampType(), False),
            StructField("pickup_borough", StringType(), False),
            StructField("split", StringType(), False),
            StructField("next_hour_demand", DoubleType(), False),
            StructField("demand_lag_1h", DoubleType(), False),
            StructField("demand_lag_24h", DoubleType(), False),
            StructField("rolling_mean_24h", DoubleType(), False),
            StructField("prior_hour_pickup_temperature_c", DoubleType(), True),
            StructField("prior_hour_pickup_humidity_pct", DoubleType(), True),
            StructField("prior_hour_pickup_precipitation_mm", DoubleType(), True),
            StructField("prior_hour_pickup_weather_code", IntegerType(), True),
            StructField("prior_hour_pickup_wind_speed_kmh", DoubleType(), True),
            StructField("prior_hour_pickup_pm25", DoubleType(), True),
            StructField("prior_hour_pickup_weather_matched", BooleanType(), True),
            StructField("prior_hour_pickup_pm25_source", StringType(), True),
        ])

    def _frames(self, demand: float = 4.0):
        row = {
            "pickup_location_id": 1,
            "target_hour": datetime(2024, 3, 1),
            "pickup_borough": "Manhattan",
            "split": "validation",
            "next_hour_demand": demand,
            "demand_lag_1h": 3.0,
            "demand_lag_24h": 2.0,
            "rolling_mean_24h": 2.5,
            "prior_hour_pickup_temperature_c": 10.0,
            "prior_hour_pickup_humidity_pct": 50.0,
            "prior_hour_pickup_precipitation_mm": 0.0,
            "prior_hour_pickup_weather_code": 1,
            "prior_hour_pickup_wind_speed_kmh": 4.0,
            "prior_hour_pickup_pm25": 7.0,
            "prior_hour_pickup_weather_matched": True,
            "prior_hour_pickup_pm25_source": "borough",
        }
        ml = self.spark.createDataFrame([row], self._ml_schema())
        features_schema = StructType([
            StructField("pickup_location_id", IntegerType(), False),
            StructField("target_hour", TimestampType(), False),
            StructField("label", DoubleType(), False),
            StructField("features", VectorUDT(), False),
        ])
        features = self.spark.createDataFrame([{
            "pickup_location_id": 1,
            "target_hour": datetime(2024, 3, 1),
            "label": demand,
            "features": Vectors.dense([1.0, 2.0]),
        }], features_schema)
        return ml, features

    def test_common_snapshot_must_be_version_zero(self) -> None:
        config = self.config()
        config["platform_comparison"]["platform_delta_version"] = 1
        with self.assertRaisesRegex(ValueError, "version = 0"):
            _comparison_spec(config)

    def test_equivalence_accepts_common_contract_and_detects_mismatch(self) -> None:
        raw_ml, raw_features = self._frames()
        platform_ml, platform_features = self._frames()
        self.assertTrue(
            compare_ml_inputs(raw_ml, platform_ml, raw_features, platform_features, 0.01)["passed"]
        )
        mismatched_ml, mismatched_features = self._frames(demand=5.0)
        self.assertFalse(
            compare_ml_inputs(raw_ml, mismatched_ml, raw_features, mismatched_features, 0.01)["passed"]
        )

    def test_metadata_and_linear_configuration_are_deterministic(self) -> None:
        config = self.config()
        first, second = comparison_metadata(config), comparison_metadata(config)
        self.assertEqual(first, second)
        self.assertEqual(first["approach_a_raw"]["source_datasets_read"], 4)
        self.assertEqual(first["approach_b_platform"]["integration_join_count"], 0)
        self.assertFalse(first["production_delta_writes"])
        self.assertEqual(
            config["model_training"]["candidates"]["linear_regression"],
            {"max_iter": 100, "reg_param": 0.01, "elastic_net_param": 0.0},
        )

    def test_production_version_snapshot_excludes_week4_ml_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            production_log = root / "silver" / "table" / "_delta_log"
            ml_log = root / "ml" / "features" / "_delta_log"
            production_log.mkdir(parents=True)
            ml_log.mkdir(parents=True)
            (production_log / "00000000000000000003.json").touch()
            (ml_log / "00000000000000000009.json").touch()
            self.assertEqual(
                production_delta_versions(root),
                {"silver/table": 3},
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
