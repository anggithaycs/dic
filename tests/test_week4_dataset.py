"""Focused tests for the causal Week 4 hourly-demand dataset builder."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import functions as F
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
from src.week4.dataset import build_hourly_demand_dataset


class Week4DatasetTests(unittest.TestCase):
    """Validate target chronology, complete-grid behavior, and leakage exclusions."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_spark_session(
            app_name="Week4DatasetTests", master="local[2]", shuffle_partitions=2
        )
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tempdir.name)
        cls.source_path = str(cls.root / "integrated")
        cls._write_source()

    @classmethod
    def tearDownClass(cls) -> None:
        stop_spark_session(cls.spark)
        cls.tempdir.cleanup()

    @classmethod
    def _write_source(cls) -> None:
        schema = StructType(
            [
                StructField("pickup_location_id", IntegerType(), False),
                StructField("pickup_borough", StringType(), True),
                StructField("pickup_local_datetime", TimestampType(), False),
                StructField("pickup_temperature_c", DoubleType(), True),
                StructField("pickup_humidity_pct", DoubleType(), True),
                StructField("pickup_precipitation_mm", DoubleType(), True),
                StructField("pickup_weather_code", IntegerType(), True),
                StructField("pickup_wind_speed_kmh", DoubleType(), True),
                StructField("pickup_pm25", DoubleType(), True),
                StructField("pickup_weather_matched", BooleanType(), True),
                StructField("pickup_pm25_source", StringType(), True),
                StructField("trip_id", StringType(), False),
                StructField("fare_amount", DoubleType(), True),
            ]
        )
        start = datetime(2024, 1, 1)
        rows = []
        for offset in range(72):
            if offset == 3:
                continue
            timestamp = start + timedelta(hours=offset)
            rows.append(
                (
                    1,
                    "Manhattan",
                    timestamp,
                    float(offset),
                    50.0,
                    None,
                    1,
                    4.0,
                    float(100 + offset),
                    True,
                    "borough",
                    f"zone1-{offset}",
                    12.0,
                )
            )
            if offset == 1:
                rows.append(
                    (
                        1,
                        "Manhattan",
                        timestamp,
                        float(offset),
                        50.0,
                        None,
                        1,
                        4.0,
                        float(100 + offset),
                        True,
                        "borough",
                        "zone1-extra",
                        12.0,
                    )
                )
        # A single row establishes a second zone; the complete grid supplies its zeros.
        rows.append(
            (2, "Queens", start, 1.0, 60.0, None, 2, 5.0, 11.0, True, "citywide", "zone2-0", 15.0)
        )
        cls.spark.createDataFrame(rows, schema).write.format("delta").save(
            cls.source_path
        )

    def config(self) -> dict:
        return {
            "source": {
                "delta_path": self.source_path,
                "delta_version": 0,
                "decision_timestamp_column": "pickup_local_datetime",
                "pickup_zone_column": "pickup_location_id",
                "pickup_borough_column": "pickup_borough",
            },
            "output": {
                "delta_path": str(self.root / "output"),
                "evidence_path": str(self.root / "evidence.json"),
            },
            "target": {"name": "next_hour_demand", "horizon_hours": 1},
            "lags": {
                "include_rolling_mean_24h": True,
                "drop_rows_without_lag_24h": False,
            },
            "splits": {
                "train": {"start": "2024-01-01", "end": "2024-01-01"},
                "validation": {"start": "2024-01-02", "end": "2024-01-02"},
                "test": {"start": "2024-01-03", "end": "2024-01-03"},
            },
            "environmental_features": [
                "pickup_temperature_c",
                "pickup_humidity_pct",
                "pickup_precipitation_mm",
                "pickup_weather_code",
                "pickup_wind_speed_kmh",
                "pickup_pm25",
                "pickup_weather_matched",
                "pickup_pm25_source",
            ],
            "leakage_columns": ["trip_id", "fare_amount"],
        }

    def dataset(self):
        return build_hourly_demand_dataset(self.spark, self.config())

    def row(self, zone: int, hour: int):
        timestamp = datetime(2024, 1, 1) + timedelta(hours=hour)
        return self.dataset().filter(
            (F.col("pickup_location_id") == zone)
            & (F.col("decision_hour") == F.lit(timestamp))
        ).first()

    def test_next_hour_target_and_zero_demand_rows_are_retained(self) -> None:
        # Hour 3 is absent in source data, so hour 2's next-hour target is zero.
        self.assertEqual(self.row(1, 2).next_hour_demand, 0)
        # The absent source hour remains a grid row and forecasts hour 4 demand.
        self.assertEqual(self.row(1, 3).next_hour_demand, 1)
        self.assertIsNotNone(self.row(2, 10))

    def test_lags_and_rolling_window_are_relative_to_target_hour(self) -> None:
        # This row predicts target hour 05:00. Its lag_1h is demand at 04:00.
        row = self.row(1, 4)
        self.assertEqual(row.target_hour, datetime(2024, 1, 1, 5))
        self.assertEqual(row.demand_lag_1h, 1)

        # This row predicts 02 Jan 02:00. T - 24h is 01 Jan 02:00 (one trip),
        # whereas the former decision-relative calculation would have used 01:00.
        row = self.row(1, 25)
        self.assertEqual(row.target_hour, datetime(2024, 1, 2, 2))
        self.assertEqual(row.demand_lag_24h, 1)

        # For target hour 02 Jan 02:00, rolling uses demand from 01 Jan 02:00
        # through 02 Jan 01:00: 23 trips across 24 grid hours (hour 03 is zero).
        self.assertAlmostEqual(row.rolling_mean_24h, 23.0 / 24.0)

    def test_environmental_context_is_exactly_one_hour_before_target(self) -> None:
        # Source temperature and PM2.5 encode their source-hour offset.  The
        # target at 05:00 must use 04:00 context, not the older 03:00 context.
        row = self.row(1, 4)
        self.assertEqual(row.target_hour, datetime(2024, 1, 1, 5))
        self.assertEqual(row.prior_hour_pickup_temperature_c, 4.0)
        self.assertEqual(row.prior_hour_pickup_pm25, 104.0)

    def test_chronological_splits_do_not_overlap(self) -> None:
        rows = self.dataset().select(
            "split", F.date_format("target_hour", "yyyy-MM-dd").alias("target_date")
        ).collect()
        self.assertTrue(rows)
        dates_by_split = {}
        for row in rows:
            dates_by_split.setdefault(row["split"], set()).add(row["target_date"])
        self.assertEqual(dates_by_split["train"], {"2024-01-01"})
        self.assertEqual(dates_by_split["validation"], {"2024-01-02"})
        self.assertEqual(dates_by_split["test"], {"2024-01-03"})

    def test_output_excludes_leakage_columns_and_is_deterministic(self) -> None:
        first = self.dataset()
        second = self.dataset()
        self.assertEqual(first.schema, second.schema)
        self.assertEqual(first.count(), second.count())
        self.assertNotIn("trip_id", first.columns)
        self.assertNotIn("fare_amount", first.columns)
        self.assertNotIn("demand", first.columns)
        self.assertIn("prior_hour_pickup_temperature_c", first.columns)
        self.assertIn("prior_hour_pickup_temperature_c_is_missing", first.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
