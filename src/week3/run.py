"""Week 3 CLI and sequential batch orchestration."""

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

from src.common.config import load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.week3.generate import generate
from src.week3.refresh import refresh
from src.week3.updates import append_record, process


def disk_bytes(path):
    return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file())


def execute(spark, config, releases, root, gold, validate=True, monitor=True):
    root, gold = Path(root), Path(gold)
    root.mkdir(parents=True, exist_ok=True)
    # Single writer only. Recover cross-table failures with a full analytical refresh.
    dirty = root / "refresh_pending.json"
    repair = dirty.exists()
    run_id = uuid4().hex
    dirty.write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    started = time.perf_counter()
    metrics, impacts = {}, {}
    
    try:
        for name in ("taxi_zones", "weather", "air_quality", "taxi_trips"):
            if name in releases:
                metrics[name], impacts[name] = process(
                    spark, config, name, releases[name], root, run_id, validate, monitor
                )
    
        update_seconds = time.perf_counter() - started
        refreshed = refresh(
            spark, config, impacts, root, gold, run_id, repair=repair, monitor=monitor
        )
    
        status = (
            "success"
            if all(m["status"] == "success" for m in metrics.values())
            else "completed_with_rejections"
        )
    
        result = {
            "run_id": run_id,
            "status": status,
            "datasets": metrics,
            "incremental_update_seconds": update_seconds,
            **refreshed,
            "total_seconds": time.perf_counter() - started,
            "recovery_refresh": repair,
        }
    
        (root / f"{run_id}.json").write_text(
            json.dumps(result, indent=2), encoding="utf-8"
        )
    
        if monitor:
            append_record(
                spark,
                root / "pipeline_runs",
                {
                    "run_id": run_id,
                    "status": status,
                    "duration_seconds": result["total_seconds"],
                    "incremental_update_seconds": update_seconds,
                    "analytical_refresh_seconds": result["analytical_refresh_seconds"],
                    "error": "",
                },
            )
    
        dirty.unlink()
        return result
    
    except Exception as error:
        if monitor:
            append_record(
                spark,
                root / "pipeline_runs",
                {
                    "run_id": run_id,
                    "status": "failed",
                    "duration_seconds": time.perf_counter() - started,
                    "incremental_update_seconds": 0.0,
                    "analytical_refresh_seconds": 0.0,
                    "error": str(error)[:2000],
                },
            )
        raise


def release_paths(directory, config):
    return {
        name: str(path)
        for name, spec in config["datasets"].items()
        if (path := Path(directory) / f"{name}.{spec['format']}").exists()
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["generate", "update", "monitor", "evaluate"]
    )
    parser.add_argument("--config", default="config/platform_config.yaml")
    parser.add_argument("--updates", default="data/week3_release")
    parser.add_argument("--operations", default="storage/delta/week3")
    parser.add_argument("--gold", default="storage/delta/gold")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default="storage/metrics/week3")
    args = parser.parse_args()
    config = load_config(args.config)
    spark = get_spark_session(**config["spark"])
    
    try:
        if args.command == "generate":
            result = generate(spark, config, args.updates, args.hours)
        elif args.command == "update":
            releases = release_paths(args.updates, config)
            if not releases:
                raise ValueError("No update files found")
            result = execute(spark, config, releases, args.operations, args.gold)
        elif args.command == "evaluate":
            from src.week3.evaluate import evaluate

            result = evaluate(
                spark,
                config,
                release_paths(args.updates, config),
                args.gold,
                args.output,
                args.repeats,
            )
        else:
            spark.read.format("delta").load(
                str(Path(args.operations) / "executions")
            ).createOrReplaceTempView("executions")
            sql = Path(__file__).with_name("monitoring.sql").read_text(encoding="utf-8")
    
            for statement in sql.split(";"):
                if statement.strip():
                    spark.sql(statement).show(truncate=False)
    
            result = {"monitoring": "complete"}
        print(json.dumps(result, indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
