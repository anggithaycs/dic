"""Load platform settings and construct dataset ingestors."""

import re
from pathlib import Path
from typing import Any

import yaml

from src.ingestion.air_quality_ingestion import AirQualityIngestor
from src.ingestion.base_ingestion import BaseDatasetIngestor
from src.ingestion.taxi_ingestion import TaxiTripsIngestor
from src.ingestion.weather_ingestion import WeatherIngestor
from src.ingestion.zone_ingestion import TaxiZoneIngestor

INGESTOR_REGISTRY: dict[str, type[BaseDatasetIngestor]] = {
    "taxi_zones": TaxiZoneIngestor,
    "weather": WeatherIngestor,
    "air_quality": AirQualityIngestor,
    "taxi_trips": TaxiTripsIngestor,
}


def load_config(path: str | Path) -> dict[str, Any]:
    """Loads and validates platform YAML configuration."""
    with Path(path).open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    validate_config(config)
    return config


def _require(mapping: dict, keys: tuple[str, ...], label: str) -> None:
    missing = sorted(set(keys) - mapping.keys())
    if missing:
        raise ValueError(f"{label}: missing configuration keys: {missing}")


def _strings(value: Any, label: str, allow_empty: bool = False) -> None:
    if not isinstance(value, list) or any(
        not isinstance(v, str) or not v.strip() for v in value
    ):
        raise ValueError(f"{label}: expected a list of column names")
    if (not allow_empty and not value) or len(value) != len(set(value)):
        raise ValueError(f"{label}: columns must be unique and nonempty")


def _positive(value: Any, label: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{label}: must be a positive integer")


def _validate_dataset(name: str, spec: dict[str, Any]) -> list[str]:
    """Validate source declarations without starting Spark."""
    output_paths = []

    if not isinstance(spec, dict):
        raise ValueError(f"{name}: expected a dataset mapping")

    _require(
        spec,
        (
            "source_path",
            "bronze_path",
            "silver_path",
            "quarantine_path",
            "format",
            "timezone",
            "partition_cols",
            "required_columns",
            "rename",
            "casts",
            "key",
            "quality_rules",
            "validation",
        ),
        name,
    )

    if spec["format"] not in ("csv", "parquet"):
        raise ValueError(f"{name}: unsupported format {spec['format']}")

    for key in ("source_path", "timezone"):
        if not isinstance(spec[key], str) or not spec[key].strip():
            raise ValueError(f"{name}.{key}: expected a nonempty string")

    for key in ("rename", "casts", "quality_rules", "validation"):
        if not isinstance(spec[key], dict):
            raise ValueError(f"{name}.{key}: expected a mapping")

    for key in ("required_columns", "key", "partition_cols"):
        _strings(spec[key], f"{name}.{key}", allow_empty=key == "partition_cols")

    if any(
        not isinstance(k, str) or not isinstance(v, str)
        for k, v in spec["rename"].items()
    ):
        raise ValueError(f"{name}.rename: names must be strings")

    standardized = {
        spec["rename"].get(
            c,
            re.sub(
                r"[^a-z0-9]+",
                "_",
                re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", c).lower(),
            ).strip("_"),
        )
        for c in spec["required_columns"]
    }

    if not set(spec["casts"]) <= standardized:
        raise ValueError(f"{name}.casts: use standardized required source columns")

    if any(
        dtype
        not in {"int", "bigint", "double", "string", "boolean", "date", "timestamp"}
        for dtype in spec["casts"].values()
    ):
        raise ValueError(f"{name}.casts: unsupported data type")

    for rule, sql in spec["validation"].items():
        if not isinstance(rule, str) or not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"{name}.validation: expected named SQL predicates")
        try:
            sql.format(**{**spec["quality_rules"], "source_timezone": spec["timezone"]})
        except (KeyError, ValueError, IndexError) as error:
            raise ValueError(
                f"{name}.validation.{rule}: invalid placeholder"
            ) from error

    if "required_values" in spec:
        _strings(spec["required_values"], f"{name}.required_values")

    if "reference_checks" in spec:
        checks = spec["reference_checks"]
        if not isinstance(checks, list):
            raise ValueError(f"{name}.reference_checks: expected a list")
        for index, check in enumerate(checks):
            if not isinstance(check, dict):
                raise ValueError(f"{name}.reference_checks[{index}]: expected a mapping")
            _require(
                check,
                ("source_field", "reference_path", "reference_field", "rule_name"),
                f"{name}.reference_checks[{index}]",
            )
            if any(
                not isinstance(check[key], str) or not check[key].strip()
                for key in ("source_field", "reference_path", "reference_field", "rule_name")
            ):
                raise ValueError(
                    f"{name}.reference_checks[{index}]: expected nonempty strings"
                )

    if "schema_compatibility" in spec:
        compatibility = spec["schema_compatibility"]
        if not isinstance(compatibility, dict):
            raise ValueError(f"{name}.schema_compatibility: expected a mapping")
        _require(
            compatibility,
            ("expected_canonical_fields", "supported_additive_fields"),
            f"{name}.schema_compatibility",
        )
        _strings(
            compatibility["expected_canonical_fields"],
            f"{name}.schema_compatibility.expected_canonical_fields",
        )
        _strings(
            compatibility["supported_additive_fields"],
            f"{name}.schema_compatibility.supported_additive_fields",
            allow_empty=True,
        )
        overlap = set(compatibility["expected_canonical_fields"]) & set(
            compatibility["supported_additive_fields"]
        )
        if overlap:
            raise ValueError(
                f"{name}.schema_compatibility: fields cannot be both required and additive: "
                f"{sorted(overlap)}"
            )

    if name != "taxi_trips":
        _require(spec, ("selected_columns",), name)
        _strings(spec["selected_columns"], f"{name}.selected_columns")

    output_paths.extend(spec[k] for k in ("silver_path", "quarantine_path"))

    if spec["bronze_path"] is not None:
        output_paths.append(spec["bronze_path"])

    return output_paths


def validate_config(config: dict[str, Any]) -> None:
    """Validate the current Week 1 contract before Spark starts or tables change."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a mapping")

    required_sections = (
        "platform",
        "spark",
        "paths",
        "datasets",
        "integration",
        "benchmark",
    )

    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing configuration section: {section}")
        if not isinstance(config[section], dict):
            raise TypeError(f"Configuration section '{section}' must be a dictionary")

    _require(config["platform"], ("version",), "platform")

    if (
        not isinstance(config["platform"]["version"], str)
        or not config["platform"]["version"]
    ):
        raise ValueError("platform.version: expected a nonempty string")

    _require(
        config["spark"],
        ("app_name", "master", "driver_memory", "shuffle_partitions"),
        "spark",
    )
    _positive(config["spark"]["shuffle_partitions"], "spark.shuffle_partitions")
    unknown_spark = set(config["spark"]) - {
        "app_name",
        "master",
        "driver_memory",
        "shuffle_partitions",
        "enable_aqe",
    }

    if unknown_spark:
        raise ValueError(f"Unknown Spark settings: {sorted(unknown_spark)}")

    for key in ("app_name", "master", "driver_memory"):
        if (
            not isinstance(config["spark"][key], str)
            or not config["spark"][key].strip()
        ):
            raise ValueError(f"spark.{key}: expected a nonempty string")

    if (
        "enable_aqe" in config["spark"]
        and type(config["spark"]["enable_aqe"]) is not bool
    ):
        raise ValueError("spark.enable_aqe: expected a boolean")

    _require(
        config["paths"], ("metadata_path", "metrics_path", "benchmark_report"), "paths"
    )

    if set(config["datasets"]) != set(INGESTOR_REGISTRY):
        raise ValueError(
            f"Week 1 requires exactly these datasets: {sorted(INGESTOR_REGISTRY)}"
        )

    output_paths = list(config["paths"].values())

    for name, spec in config["datasets"].items():
        output_paths.extend(_validate_dataset(name, spec))

    _require(config["datasets"]["taxi_trips"], ("column_defaults",), "taxi_trips")
    defaults = config["datasets"]["taxi_trips"]["column_defaults"]

    if not isinstance(defaults, dict):
        raise ValueError("taxi_trips.column_defaults: expected a mapping")

    _require(
        defaults,
        ("passenger_count", "tip_amount", "tolls_amount"),
        "taxi_trips.column_defaults",
    )

    air = config["datasets"]["air_quality"]
    _require(air, ("county_borough_mapping",), "air_quality")
    _require(
        air["quality_rules"],
        ("ny_state_code", "nyc_county_codes"),
        "air_quality.quality_rules",
    )

    if not air["county_borough_mapping"] or not isinstance(
        air["county_borough_mapping"], dict
    ):
        raise ValueError(
            "air_quality.county_borough_mapping: expected a nonempty mapping"
        )

    _require(config["integration"], ("output_path", "partition_cols"), "integration")
    _strings(
        config["integration"]["partition_cols"],
        "integration.partition_cols",
        allow_empty=True,
    )
    output_paths.append(config["integration"]["output_path"])
    _require(config["benchmark"], ("iterations", "storage_paths"), "benchmark")
    _positive(config["benchmark"]["iterations"], "benchmark.iterations")
    storage_paths = config["benchmark"]["storage_paths"]

    if not isinstance(storage_paths, dict):
        raise ValueError("benchmark.storage_paths: expected a mapping")

    _require(
        storage_paths,
        (
            "strategy_a_unpartitioned",
            "strategy_b_partitioned",
            "strategy_c_monthly_partitioned",
        ),
        "benchmark.storage_paths",
    )

    output_paths.extend(storage_paths.values())
    if any(not isinstance(p, str) or not p.strip() for p in output_paths):
        raise ValueError("Output paths must be nonempty strings")

    resolved = [Path(p).resolve() for p in output_paths]
    if len(set(resolved)) != len(resolved):
        raise ValueError("Output paths must be distinct")


def build_ingestors(
    spark: Any, config: dict[str, Any]
) -> dict[str, BaseDatasetIngestor]:
    """Constructs dataset ingestors from unified platform configuration."""
    validate_config(config)
    datasets_cfg = config["datasets"]
    metadata_path = config["paths"]["metadata_path"]
    schema_version = config["platform"]["version"]

    result: dict[str, BaseDatasetIngestor] = {}
    for name, cls in INGESTOR_REGISTRY.items():
        spec = datasets_cfg[name]
        result[name] = cls(
            spark=spark,
            dataset_spec=spec,
            schema_version=schema_version,
            metadata_path=metadata_path,
        )
    return result
