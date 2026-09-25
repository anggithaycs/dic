import json
import time
from copy import deepcopy
from pathlib import Path

import yaml
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import INGESTOR_REGISTRY


def settings():
    path = Path(__file__).resolve().parents[2] / "config/week3.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def ingestor(spark, config, name, source=None):
    spec = deepcopy(config["datasets"][name])

    if source is not None:
        spec["source_path"] = str(source)

    spec["validation"].update(settings()["validation"].get(name, {}))

    return INGESTOR_REGISTRY[name](spark, spec, "3.0", config["paths"]["metadata_path"])


def append_record(spark, path, record):
    frame = (
        spark.range(1)
        .select(
            *[
                F.lit(json.dumps(v) if isinstance(v, (dict, list)) else v).alias(k)
                for k, v in record.items()
            ]
        )
        .withColumn("completed_at", F.current_timestamp())
    )

    frame.coalesce(1).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(path))


def merge(spark, path, frame, keys):
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        DeltaTable.forPath(spark, str(path))
        .alias("t")
        .merge(frame.alias("s"), condition)
        .withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def classify(loader, raw, config):
    """Use existing SQL rules and add foreign-key and explicit-correction checks."""
    frame = loader.prepare(raw)

    if loader.dataset_name == "TaxiTrips":
        zones = (
            loader.spark.read.format("delta")
            .load(config["datasets"]["taxi_zones"]["silver_path"])
            .select("location_id")
        )

        for endpoint in ("pickup", "dropoff"):
            key = f"_{endpoint}_reference"
            frame = frame.join(
                F.broadcast(zones.withColumnRenamed("location_id", key)),
                F.col(f"{endpoint}_location_id") == F.col(key),
                "left",
            )
            loader.dataset_spec["validation"][f"missing_reference:{endpoint}"] = (
                f"{key} IS NOT NULL"
            )

        if "original_trip_id" in frame.columns:
            existing = loader.spark.read.format("delta").load(loader.delta_output_path)
            frame = frame.join(
                existing.select(F.col("trip_id").alias("_original_id")),
                F.col("original_trip_id") == F.col("_original_id"),
                "left",
            ).withColumn("trip_id", F.coalesce("original_trip_id", "trip_id"))
            loader.dataset_spec["validation"]["unknown_correction_id"] = (
                "original_trip_id IS NULL OR _original_id IS NOT NULL"
            )

    return loader.classify_records(frame)


def quarantine(spark, root, name, frame, run_id):
    # JSON envelopes also accept batches with missing columns or incompatible types.
    payload = F.to_json(F.struct(*[F.col(f"`{c}`") for c in frame.columns]))
    (
        frame.select(payload.alias("record"), "_rejection_reason")
        .withColumn("run_id", F.lit(run_id))
        .withColumn("rejected_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(str(root / "quarantine" / name))
    )


def process(spark, config, name, source, root, run_id, validate=True, monitor=True):
    started = time.perf_counter()
    root = Path(root)
    metric = dict(
        run_id=run_id,
        dataset=name,
        status="failed",
        schema_version="3.0",
        processed_records=0,
        inserted_records=0,
        updated_records=0,
        rejected_records=0,
        duplicate_records=0,
        out_of_scope_records=0,
        validation_failures=0,
        validation_seconds=0.0,
        duration_seconds=0.0,
        error="",
        source=str(source),
    )

    loader = ingestor(spark, config, name, source)
    raw = classified = changed = None

    try:
        # Read without the Week 1 schema gate so unsupported batches can be isolated.
        reader = spark.read.option("header", True).option("inferSchema", False)
        raw = reader.format(loader.file_format).load(str(source)).cache()
        metric["processed_records"] = raw.count()
        baseline = (
            reader.format(loader.file_format)
            .load(config["datasets"][name]["source_path"])
            .schema
        )
        additions = settings()["additions"].get(name, {})
        allowed = set(baseline.names) | set(additions)

        if name == "taxi_trips":
            allowed.add("original_trip_id")

        problems = []

        if set(baseline.names) - set(raw.columns):
            problems.append("missing_source_columns")

        if set(raw.columns) - allowed:
            problems.append("unsupported_columns")

        if loader.file_format == "parquet":
            types = {f.name: f.dataType for f in baseline}
            if any(f.name in types and f.dataType != types[f.name] for f in raw.schema):
                problems.append("unsupported_type_change")

        if problems:
            metric["rejected_records"] = metric["processed_records"]
            metric["validation_failures"] = metric["processed_records"]
            metric["status"] = "rejected_schema"
            metric["error"] = ";".join(problems)
            quarantine(
                spark,
                root,
                name,
                raw.withColumn("_rejection_reason", F.lit(metric["error"])),
                run_id,
            )
            return metric, None

        for col, rule in additions.items():
            if col in raw.columns:
                loader.dataset_spec["casts"][col] = rule["type"]
                loader.dataset_spec["validation"][f"valid_{col}"] = rule["rule"]
                loader.dataset_spec["selected_columns"].append(col)
        validation_start = time.perf_counter()

        if validate:
            classified = classify(loader, raw, config).cache()
        else:
            loader.dataset_spec["validation"] = {}
            classified = loader.classify_records(loader.prepare(raw)).cache()

        valid, rejected = loader.split_records(classified)
        valid = valid.drop(
            "_pickup_reference",
            "_dropoff_reference",
            "_original_id",
            "original_trip_id",
        )

        scoped = classified.count()
        metric["rejected_records"] = rejected.count()
        metric["validation_failures"] = rejected.filter(
            F.col("_rejection_reason") != "duplicate_key"
        ).count()

        metric["duplicate_records"] = (
            metric["rejected_records"] - metric["validation_failures"]
        )

        metric["out_of_scope_records"] = metric["processed_records"] - scoped
        metric["validation_seconds"] = time.perf_counter() - validation_start

        if metric["rejected_records"]:
            quarantine(spark, root, name, rejected, run_id)

        target = spark.read.format("delta").load(loader.delta_output_path)
        # An older release omitting an already-added field must preserve its value.
        keys = loader.dataset_spec["key"]
        missing = [c for c in target.columns if c not in valid.columns]

        if missing:
            valid = valid.join(target.select(*keys, *missing), keys, "left")
        added = [c for c in valid.columns if c not in target.columns]

        for col in added:
            target = target.withColumn(
                col, F.lit(None).cast(valid.schema[col].dataType)
            )

        joined = valid.alias("s").join(target.alias("t"), keys, "left")
        is_new = F.col(f"t.{keys[0]}").isNull()
        different = F.lit(False)

        for col in valid.columns:
            if col not in keys:
                different = different | ~F.col(f"s.`{col}`").eqNullSafe(
                    F.col(f"t.`{col}`")
                )

        changed = joined.filter(is_new | different).select("s.*").cache()
        metric["inserted_records"] = joined.filter(is_new).count()
        changed_count = changed.count()
        metric["updated_records"] = changed_count - metric["inserted_records"]
        metric["duplicate_records"] += valid.count() - changed_count
        # Persist BEFORE MERGE: Delta invalidates cached plans that read its target.
        impact_path = root / "impacts" / run_id / name
        changed.write.format("delta").mode("overwrite").save(str(impact_path))
        staged = spark.read.format("delta").load(str(impact_path))

        if changed_count or added:
            merge(spark, loader.delta_output_path, staged, keys)
        metric["status"] = "success"
        metric["schema_version"] = (
            "3.1" if set(additions) & set(valid.columns) else "3.0"
        )

        return metric, staged
    except Exception as error:
        metric["error"] = str(error)[:2000]
        raise
    finally:
        metric["duration_seconds"] = time.perf_counter() - started
        
        for frame in (raw, classified, changed):
            if frame is not None:
                frame.unpersist()
        
        if monitor:
            append_record(spark, root / "executions", metric)
