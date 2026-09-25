"""Refresh affected trips, then replace complete affected aggregate groups."""

import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.week2.analysis import prepare_views
from src.week2.products import PRODUCTS
from src.week3.updates import append_record, merge

DEPENDENCIES = {
    "daily_mobility_summary": {"taxi_trips", "taxi_zones"},
    "taxi_zone_statistics": {"taxi_trips", "taxi_zones"},
    "weather_impact_summary": {"taxi_trips", "taxi_zones", "weather"},
    "air_quality_impact_summary": {"taxi_trips", "weather", "air_quality"},
}


def refresh(
    spark, config, impacts, root, gold_root, run_id, repair=False, monitor=True
):
    started = time.perf_counter()
    pipeline = UrbanDataIntegrationPipeline.from_config(spark, config)
    sources = pipeline.load_sources()
    taxi = sources["taxi"]
    changed = {
        name for name, df in impacts.items() if df is not None and df.limit(1).count()
    }
    output = config["integration"]["output_path"]
    old = spark.read.format("delta").load(output)
    ids = taxi.limit(0).select("trip_id")
    
    if repair:
        ids = taxi.select("trip_id")
        changed = set(config["datasets"])
    else:
        for name in changed:
            frame = impacts[name]
    
            if name == "taxi_trips":
                affected = frame.select("trip_id")
            elif name == "taxi_zones":
                refs = frame.select("location_id")
                affected = taxi.join(
                    refs,
                    (
                        (taxi.pickup_location_id == refs.location_id)
                        | (taxi.dropoff_location_id == refs.location_id)
                    ),
                    "left_semi",
                ).select("trip_id")
            else:
                hours = frame.select("observation_datetime").distinct()
                affected = taxi.join(
                    hours,
                    (
                        (
                            F.date_trunc("hour", taxi.pickup_datetime)
                            == hours.observation_datetime
                        )
                        | (
                            F.date_trunc("hour", taxi.dropoff_datetime)
                            == hours.observation_datetime
                        )
                    ),
                    "left_semi",
                ).select("trip_id")
            ids = ids.unionByName(affected)
        ids = ids.distinct()
    
    selected = taxi.join(ids, "trip_id", "left_semi").cache()
    count = selected.count()
    dates = (
        old.join(ids, "trip_id", "left_semi")
        .select("pickup_date")
        .union(selected.select("pickup_date"))
        .distinct()
        .collect()
    )
    
    if count:
        enriched = pipeline.build_integrated_dataset(
            selected, sources["weather"], sources["air"], sources["zones"]
        ).cache()
    
        try:
            pipeline.assert_trip_identity(selected, enriched)
            merge(spark, output, enriched, ["trip_id"])
        finally:
            enriched.unpersist()
    
    selected.unpersist()
    integration_seconds = time.perf_counter() - started
    product_start = time.perf_counter()
    prepare_views(spark, spark.read.format("delta").load(output))
    results = {}
    gold_root = Path(gold_root)
    
    for name, sql in PRODUCTS.items():
        path = str(gold_root / name)
        exists = DeltaTable.isDeltaTable(spark, path)
    
        if exists and not repair and not (changed & DEPENDENCIES[name]):
            results[name] = {"status": "skipped", "seconds": 0.0}
            continue
    
        start = time.perf_counter()
        product = spark.sql(sql)
        predicate = None
    
        if (
            exists
            and not repair
            and dates
            and name in ("daily_mobility_summary", "taxi_zone_statistics")
        ):
            col = "pickup_date" if name == "daily_mobility_summary" else "month"
            values = sorted(
                {
                    str(
                        r.pickup_date
                        if col == "pickup_date"
                        else r.pickup_date.replace(day=1)
                    )
                    for r in dates
                }
            )
            predicate = col + " IN (" + ",".join(f"DATE '{v}'" for v in values) + ")"
            product = product.filter(predicate)
        elif exists and not repair and not count:
            results[name] = {"status": "skipped_no_affected_trips", "seconds": 0.0}
            continue
    
        writer = product.coalesce(1).write.format("delta").mode("overwrite")
        if predicate:
            writer = writer.option("replaceWhere", predicate)
        else:
            writer = writer.option("overwriteSchema", True)
    
        writer.save(path)
        metadata_path = str(gold_root / "_metadata")
        metadata = spark.range(1).select(
            F.lit(name).alias("product"),
            F.lit(output).alias("data_source"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("refreshed_at"),
            F.lit("1.0").alias("schema_version"),
            F.lit(spark.read.format("delta").load(path).count())
            .cast("long")
            .alias("row_count"),
        )
    
        if DeltaTable.isDeltaTable(spark, metadata_path):
            (
                DeltaTable.forPath(spark, metadata_path)
                .alias("t")
                .merge(metadata.alias("s"), "t.product = s.product")
                .whenMatchedUpdate(
                    set={c: f"s.{c}" for c in metadata.columns if c != "created_at"}
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            metadata.write.format("delta").save(metadata_path)
    
        results[name] = {
            "status": "refreshed",
            "scope": predicate or "full",
            "seconds": time.perf_counter() - start,
        }
    
        if monitor:
            append_record(
                spark,
                Path(root) / "product_refreshes",
                {
                    "run_id": run_id,
                    "product": name,
                    "data_source": output,
                    "schema_version": "1.0",
                    "scope": predicate or "full",
                    "seconds": results[name]["seconds"],
                    "source_version": int(
                        DeltaTable.forPath(spark, output).history(1).first().version
                    ),
                },
            )
    
    return {
        "affected_trips": count,
        "integration_seconds": integration_seconds,
        "analytical_refresh_seconds": time.perf_counter() - product_start,
        "products": results,
    }
