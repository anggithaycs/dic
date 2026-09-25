"""Controlled ablations on independent local Delta copies."""

import json
import shutil
from copy import deepcopy
from pathlib import Path
from statistics import median
from uuid import uuid4

from src.week2.benchmark import check_equal
from src.week2.products import PRODUCTS
from src.week3.run import disk_bytes, execute


def clone(config, gold, destination):
    result = deepcopy(config)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    for name, spec in result["datasets"].items():
        path = destination / name
        shutil.copytree(spec["silver_path"], path)
        spec["silver_path"] = str(path)
    path = destination / "integrated"
    shutil.copytree(config["integration"]["output_path"], path)
    result["integration"]["output_path"] = str(path)
    shutil.copytree(gold, destination / "gold")
    return result


def evaluate(spark, config, releases, gold, destination, repeats=3):
    if repeats < 1 or not releases:
        raise ValueError("Evaluation requires updates and at least one repetition")
    
    destination = Path(destination) / uuid4().hex
    destination.mkdir(parents=True)
    cases = [
        ("full", True, True),
        ("without_validation", False, True),
        ("without_monitoring", True, False),
    ]
    
    measurements = []
    reference = None
    
    for repetition in range(repeats):
        for label, validation, monitoring in (
            cases[repetition % 3 :] + cases[: repetition % 3]
        ):
            folder = destination / f"{repetition}-{label}"
            cfg = clone(config, gold, folder)
            before = disk_bytes(folder)
            spark.catalog.clearCache()
            result = execute(
                spark,
                cfg,
                releases,
                folder / "operations",
                folder / "gold",
                validation,
                monitoring,
            )
            result.update(
                case=label,
                repetition=repetition,
                physical_bytes_before=before,
                physical_bytes_after=disk_bytes(folder),
            )
            result["storage_overhead_bytes"] = result["physical_bytes_after"] - before
            if (
                any(m["validation_failures"] for m in result["datasets"].values())
                or result["status"] != "success"
            ):
                raise ValueError(
                    "Use valid releases for ablations; test invalid rows separately"
                )
            if reference is None:
                reference = folder
            for name in PRODUCTS:
                expected = spark.read.format("delta").load(
                    str(reference / "gold" / name)
                )
                actual = spark.read.format("delta").load(str(folder / "gold" / name))
                keys = {
                    "daily_mobility_summary": ["pickup_date", "pickup_borough"],
                    "taxi_zone_statistics": [
                        "month",
                        "pickup_location_id",
                        "pickup_zone",
                        "pickup_borough",
                    ],
                    "weather_impact_summary": [
                        "pickup_location_id",
                        "pickup_zone",
                        "weather",
                    ],
                    "air_quality_impact_summary": ["hour_utc"],
                }[name]
                check_equal(expected.collect(), actual.collect(), keys)
            result["equivalent_products"] = True
            measurements.append(result)
            (destination / "measurements.json").write_text(
                json.dumps(measurements, indent=2), encoding="utf-8"
            )
    
    medians = {
        label: median(r["total_seconds"] for r in measurements if r["case"] == label)
        for label, _, _ in cases
    }
    
    summary = {
        "runs": len(measurements),
        "median_total_seconds": medians,
        "validation_overhead_seconds": medians["full"] - medians["without_validation"],
        "monitoring_overhead_seconds": medians["full"] - medians["without_monitoring"],
        "evidence": str(destination / "measurements.json"),
    }
    
    (destination / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    
    return summary
