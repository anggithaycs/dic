"""Isolated Week 3 acceptance test and reproducible small-data evaluation."""

import json
import sys
from pathlib import Path
from uuid import uuid4

import yaml
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import load_config
from src.common.spark_session import get_spark_session
from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.week2.analysis import prepare_views
from src.week2.products import PRODUCTS, build_products
from src.week3.generate import generate, single_file
from src.week3.run import execute, release_paths
from src.week3.updates import ingestor


def fixture(spark, folder, timestamp_type="timestamp"):
    cfg = load_config(ROOT / "config/platform_config.yaml")
    cfg["spark"].update(master="local[2]", shuffle_partitions=2, driver_memory="2g")
    folder.mkdir(parents=True)
    cfg["paths"]["metadata_path"] = str(folder / "old_audit")
    raw = {
        "taxi_trips": spark.sql("""
            SELECT 1 AS VendorID,
             TIMESTAMP '2024-03-31 23:00:00' + id * INTERVAL 1 SECOND AS tpep_pickup_datetime,
             TIMESTAMP '2024-03-31 23:10:00' + id * INTERVAL 1 SECOND AS tpep_dropoff_datetime,
             1 AS passenger_count, 1 AS PULocationID, 2 AS DOLocationID,
             2.0D AS trip_distance, 12.0D AS fare_amount, 15.0D AS total_amount,
             1.0D AS tip_amount, 0.0D AS tolls_amount FROM range(200)
        """),
        "weather": spark.sql("""
            SELECT 2024 AS year, 4 AS month, 1 AS day, 3 AS hour,
             18.0D AS temp, 60.0D AS rhum, 0.0D AS prcp, 90 AS wdir,
             5.0D AS wspd, 1010.0D AS pres, 2 AS cldc, 1 AS coco
        """),
        "air_quality": spark.sql("""
            SELECT 36 AS `State Code`, 61 AS `County Code`, 1 AS `Site Num`,
             88101 AS `Parameter Code`, 1 AS POC, 40.7D AS Latitude,
             -74.0D AS Longitude, 'PM2.5' AS `Parameter Name`,
             '2024-03-31' AS `Date Local`, '22:00' AS `Time Local`,
             '2024-04-01' AS `Date GMT`, '03:00' AS `Time GMT`,
             10.0D AS `Sample Measurement`, 'ug/m3' AS `Units of Measure`,
             'New York' AS `State Name`, 'New York' AS `County Name`
        """),
        "taxi_zones": spark.sql("""
            SELECT * FROM VALUES (1, 'Manhattan', 'A', 'Yellow'),
            (2, 'Queens', 'B', 'Yellow') AS t(LocationID, Borough, Zone, service_zone)
        """),
    }
    for name, frame in raw.items():
        if name == "taxi_trips":
            for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                frame = frame.withColumn(col, F.col(col).cast(timestamp_type))
        spec = cfg["datasets"][name]
        spec["source_path"] = str(folder / f"{name}.{spec['format']}")
        spec["silver_path"] = str(folder / name)
        spec["bronze_path"] = str(folder / f"bronze_{name}")
        spec["quarantine_path"] = str(folder / f"quarantine_{name}")
        single_file(frame, spec["source_path"], spec["format"])
        loader = ingestor(spark, cfg, name)
        valid, _ = loader.apply_data_quality_rules(
            loader.prepare(loader.load_raw_data())
        )
        loader.save_to_delta(valid)
    cfg["integration"]["output_path"] = str(folder / "integrated")
    UrbanDataIntegrationPipeline.from_config(spark, cfg).run()
    prepare_views(
        spark, spark.read.format("delta").load(cfg["integration"]["output_path"])
    )
    build_products(spark, folder / "gold", cfg["integration"]["output_path"])
    (folder / "config.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg


def main():
    folder = ROOT / ".test-output" / ("week3-" + uuid4().hex)
    spark = get_spark_session(
        master="local[2]", shuffle_partitions=2, driver_memory="2g"
    )
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    try:
        cfg = fixture(
            spark,
            folder / "baseline",
            "timestamp_ntz" if "--generation-only" in sys.argv else "timestamp",
        )
        manifest = generate(spark, cfg, folder / "release", hours=4)
        assert manifest["taxi_trips"]["new_records"] == 15
        assert manifest["taxi_trips"]["duplicate_records"] == 3
        releases = release_paths(folder / "release", cfg)
        original = spark.read.parquet(cfg["datasets"]["taxi_trips"]["source_path"])
        released = spark.read.parquet(releases["taxi_trips"])
        latest = original.agg(F.max("tpep_pickup_datetime").alias("_latest"))
        assert (
            released.crossJoin(latest)
            .filter(F.col("tpep_pickup_datetime") > F.col("_latest"))
            .count()
            == manifest["taxi_trips"]["new_records"]
        )
        assert released.schema == original.schema
        for name in ("weather", "air_quality"):
            loader = ingestor(spark, cfg, name)
            latest = loader.prepare(loader.load_raw_data()).agg(
                F.max("observation_datetime").alias("_latest")
            )
            raw_update = spark.read.option("header", True).csv(releases[name])
            earliest = loader.prepare(raw_update).agg(
                F.min("observation_datetime").alias("_earliest")
            )
            assert (
                earliest.crossJoin(latest)
                .filter("_earliest = _latest + INTERVAL 1 HOUR")
                .count()
                == 1
            )
        if "--generation-only" in sys.argv:
            print(
                "PASS: NTZ schema preserved; all new taxis follow source maximum; context starts at next hour",
                flush=True,
            )
            return

        if "--evaluate" in sys.argv:
            from src.week3.evaluate import evaluate

            result = evaluate(
                spark,
                cfg,
                releases,
                folder / "baseline/gold",
                folder / "evaluation",
                repeats=3,
            )
            print("EVALUATION", json.dumps(result), flush=True)
            return
        result = execute(
            spark, cfg, releases, folder / "operations", folder / "baseline/gold"
        )
        assert result["datasets"]["taxi_trips"]["inserted_records"] == 15, result
        assert result["datasets"]["taxi_trips"]["duplicate_records"] == 3, result
        assert result["datasets"]["weather"]["rejected_records"] == 0, result
        assert result["datasets"]["taxi_zones"]["updated_records"] == 1, result
        for name, col in (("weather", "humidity"), ("air_quality", "aqi")):
            table = spark.read.format("delta").load(
                cfg["datasets"][name]["silver_path"]
            )
            assert col in table.columns
            assert table.filter(F.col(col).isNull()).count() == 1
        repeat = execute(
            spark, cfg, releases, folder / "operations", folder / "baseline/gold"
        )
        assert all(
            m["inserted_records"] == m["updated_records"] == 0
            for m in repeat["datasets"].values()
        )
        assert all(v["status"] == "skipped" for v in repeat["products"].values()), (
            repeat
        )
        # Compare selective refresh with full recomputation of all Week 2 products.
        prepare_views(
            spark, spark.read.format("delta").load(cfg["integration"]["output_path"])
        )
        for name, sql in PRODUCTS.items():
            expected = spark.sql(sql)
            actual = spark.read.format("delta").load(
                str(folder / "baseline/gold" / name)
            )
            assert not expected.exceptAll(actual).limit(1).count(), name
            assert not actual.exceptAll(expected).limit(1).count(), name
        # A bad row is isolated while valid rows continue.
        source = spark.read.parquet(releases["taxi_trips"])
        bad = source.limit(1).withColumn("PULocationID", F.lit(9999))
        bad_file = folder / "bad.parquet"
        single_file(bad, bad_file, "parquet")
        rejected = execute(
            spark,
            cfg,
            {"taxi_trips": str(bad_file)},
            folder / "operations",
            folder / "baseline/gold",
        )
        assert rejected["datasets"]["taxi_trips"]["validation_failures"] == 1
        # Unsupported addition is a batch rejection, not a crash.
        unknown = folder / "unknown.csv"
        unknown.write_text(
            "LocationID,Borough,Zone,service_zone,unexpected\n1,Manhattan,A,Yellow,x\n"
        )
        rejected = execute(
            spark,
            cfg,
            {"taxi_zones": str(unknown)},
            folder / "operations",
            folder / "baseline/gold",
        )
        assert rejected["datasets"]["taxi_zones"]["status"] == "rejected_schema"
        # Correct an existing trip using its explicit original ID.
        original = (
            spark.read.format("delta")
            .load(cfg["datasets"]["taxi_trips"]["silver_path"])
            .orderBy("pickup_datetime")
            .first()
        )
        correction = (
            spark.read.parquet(cfg["datasets"]["taxi_trips"]["source_path"])
            .orderBy("tpep_pickup_datetime")
            .limit(1)
        )
        correction = correction.withColumn("fare_amount", F.lit(20.0)).withColumn(
            "original_trip_id", F.lit(original.trip_id)
        )
        correction_file = folder / "correction.parquet"
        single_file(correction, correction_file, "parquet")
        corrected = execute(
            spark,
            cfg,
            {"taxi_trips": str(correction_file)},
            folder / "operations",
            folder / "baseline/gold",
        )
        assert corrected["datasets"]["taxi_trips"]["updated_records"] == 1, corrected
        assert corrected["datasets"]["taxi_trips"]["inserted_records"] == 0, corrected
        # Simulate interruption after Silver commit: a subsequent run must repair Gold.
        (folder / "operations/refresh_pending.json").write_text("{}")
        recovered = execute(
            spark, cfg, {}, folder / "operations", folder / "baseline/gold"
        )
        assert recovered["recovery_refresh"]
        prepare_views(
            spark, spark.read.format("delta").load(cfg["integration"]["output_path"])
        )
        from src.week2.analysis import QUERIES, QUERY_KEYS
        from src.week2.benchmark import check_equal
        from src.week2.products import GOLD_QUERIES, load_products

        load_products(spark, folder / "baseline/gold")
        for name in QUERIES:
            check_equal(
                spark.sql(QUERIES[name]).collect(),
                spark.sql(GOLD_QUERIES[name]).collect(),
                QUERY_KEYS[name],
            )
        print(
            "PASS: generation, merge, replay, evolution, refresh equivalence, quarantine, schema rejection, correction, recovery, six legacy queries"
        )
        print("EVIDENCE", folder, flush=True)
        (folder / "acceptance.json").write_text(
            json.dumps(
                {
                    "first": result,
                    "repeat": repeat,
                    "corrected": corrected,
                    "recovered": recovered,
                    "legacy_queries_verified": 6,
                },
                indent=2,
            )
        )
    finally:
        spark.stop()


def test_week3():
    main()


if __name__ == "__main__":
    main()
