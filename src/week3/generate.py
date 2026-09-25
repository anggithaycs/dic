"""Generate one physical release file per dataset, plus a measured manifest."""

import json
import math
import shutil
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.week3.updates import ingestor


def single_file(frame, path, fmt):
    path = Path(path)
    parts = path.parent / (path.name + ".parts")
    frame.coalesce(1).write.format(fmt).option("header", True).mode("error").save(
        str(parts)
    )
    part = next(parts.glob(f"part-*.{fmt}"))
    shutil.move(str(part), str(path))
    # Remove only the temporary directory this function just created.
    shutil.rmtree(parts)


def generate(
    spark, config, destination, hours=48, new_fraction=0.075, duplicate_fraction=0.015
):
    if not 0.05 <= new_fraction <= 0.10 or not 0.01 <= duplicate_fraction <= 0.02:
        raise ValueError("Taxi fractions must be 5–10% new and 1–2% duplicate")
    
    if hours < 1:
        raise ValueError("hours must be positive")
    
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {}
    
    for name in ("taxi_zones", "taxi_trips", "weather", "air_quality"):
        loader = ingestor(spark, config, name)
    
        # Sample rows that passed the original snapshot contract.
        loader.dataset_spec["validation"] = dict(config["datasets"][name]["validation"])
        raw = loader.load_raw_data()
    
        # Retain original names and types through the existing validation pipeline.
        wrapped = raw.withColumn(
            "source", F.struct(*[F.col(f"`{c}`") for c in raw.columns])
        )
    
        clean = (
            loader.classify_records(loader.prepare(wrapped))
            .filter(F.col("_rejection_reason").isNull())
            .cache()
        )
    
        if name == "taxi_trips":
            original_count = raw.count()
            new_count = math.ceil(original_count * new_fraction)
            duplicate_count = math.ceil(original_count * duplicate_fraction)
            seeds = clean.orderBy("trip_id").limit(new_count)
    
            if seeds.count() != new_count:
                raise ValueError(
                    "Too few valid taxi rows to sample without replacement"
                )
    
            # Keep arithmetic in Spark's UTC session. Collecting naive datetimes
            # mixes source TIMESTAMP_NTZ with driver-local TIMESTAMP conversions.
            latest = raw.agg(F.unix_timestamp(F.max("tpep_pickup_datetime"))).first()[0]
            first = seeds.agg(
                F.unix_timestamp(F.min("source.tpep_pickup_datetime"))
            ).first()[0]
            shift = latest - first + 3600
            trips = seeds.select("source.*")
    
            for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                trips = trips.withColumn(
                    col, F.col(col) + F.expr(f"INTERVAL {shift} SECONDS")
                )
    
            release = trips.unionByName(
                clean.orderBy("trip_id").limit(duplicate_count).select("source.*")
            )
            schema_changes = []
            modified = 0
    
        elif name == "taxi_zones":
            release = clean.orderBy("location_id").limit(1).select("source.*")
            release = release.withColumn(
                "Zone", F.concat(F.col("Zone"), F.lit(" (updated)"))
            )
            new_count, duplicate_count, modified = 0, 0, 1
            schema_changes = []
    
        else:
            latest = clean.agg(F.max("observation_datetime").cast("string")).first()[0]
    
            if latest is None:
                raise ValueError(f"No valid seed observations for {name}")
    
            if name == "weather":
                seed = clean.orderBy(F.col("observation_datetime").desc()).limit(1)
            else:
                sensor = ["state_code", "county_code", "site_number", "poc"]
                seed = clean.withColumn(
                    "_rank",
                    F.row_number().over(
                        Window.partitionBy(*sensor).orderBy(
                            F.col("observation_datetime").desc()
                        )
                    ),
                ).filter("_rank = 1")
    
            release = seed.select("source.*").crossJoin(spark.range(hours))
            release = release.withColumn(
                "_time",
                F.to_timestamp(F.lit(latest)) + F.expr("(id + 1) * INTERVAL 1 HOUR"),
            )
    
            if name == "weather":
                for col, function in (
                    ("year", F.year),
                    ("month", F.month),
                    ("day", F.dayofmonth),
                    ("hour", F.hour),
                ):
                    release = release.withColumn(col, function("_time"))
    
                release = release.withColumn(
                    "humidity", (F.lit(55) + F.col("id") % 25).cast("double")
                )
                release = release.withColumn("rhum", F.col("humidity"))
                release = release.withColumn(
                    "temp", F.lit(18) + F.sin(F.col("id") / 24 * 6.283) * 5
                )
                schema_changes = ["humidity: double (20–100 percent)"]
    
            else:
                release = release.withColumn(
                    "Date GMT", F.date_format("_time", "yyyy-MM-dd")
                )
                release = release.withColumn(
                    "Time GMT", F.date_format("_time", "HH:mm")
                )
                local = F.col("_time") - F.expr("INTERVAL 5 HOURS")
                release = release.withColumn(
                    "Date Local", F.date_format(local, "yyyy-MM-dd")
                )
                release = release.withColumn(
                    "Time Local", F.date_format(local, "HH:mm")
                )
                release = release.withColumn(
                    "Sample Measurement", F.lit(8) + F.col("id") % 15
                )
                release = release.withColumn(
                    "aqi", (F.lit(25) + F.col("id") % 60).cast("double")
                )
                schema_changes = [
                    "aqi: double (synthetic index, not computed from PM2.5)"
                ]
            release = release.drop("id", "_time")
            new_count, duplicate_count, modified = release.count(), 0, 0
    
        path = destination / f"{name}.{loader.file_format}"
        single_file(release, path, loader.file_format)
        manifest[name] = {
            "file": path.name,
            "new_records": new_count,
            "duplicate_records": duplicate_count,
            "modified_records": modified,
            "schema_changes": schema_changes,
        }
        clean.unpersist()
    
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    
    return manifest
