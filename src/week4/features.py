"""Train-fitted Spark feature engineering for Week 4 demand forecasting."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_session import get_spark_session, stop_spark_session
from src.week4.dataset import DEFAULT_CONFIG_PATH, load_week4_config


@dataclass(frozen=True)
class FeatureTransformation:
    """A fitted preprocessing model and its all-split transformed outputs."""

    model: PipelineModel
    transformed: DataFrame
    model_ready: DataFrame


def _feature_spec(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return and validate the optional Week 4 feature-pipeline contract."""
    spec = config.get("feature_pipeline")
    if not isinstance(spec, Mapping):
        raise ValueError("Week 4 config: feature_pipeline is required for Task 2")
    required = {
        "input_delta_path",
        "output_delta_path",
        "evidence_path",
        "label_column",
        "split_column",
        "target_hour_column",
        "trace_columns",
        "categorical_features",
        "numeric_features",
        "binary_features",
        "missingness_indicators",
        "imputer",
        "scaling",
    }
    missing = sorted(required - set(spec))
    if missing:
        raise ValueError(f"Week 4 feature_pipeline: missing {missing}")
    if not all(isinstance(spec[name], list) for name in (
        "trace_columns", "categorical_features", "numeric_features", "binary_features", "missingness_indicators"
    )):
        raise ValueError("Week 4 feature lists must be lists")
    if not isinstance(spec["scaling"].get("enabled"), bool):
        raise ValueError("Week 4 feature_pipeline.scaling.enabled must be boolean")
    return spec


def feature_input_columns(config: Mapping[str, Any]) -> list[str]:
    """Return all Task 1 columns required by the configured feature pipeline."""
    spec = _feature_spec(config)
    return list(dict.fromkeys(
        [spec["label_column"], spec["split_column"], spec["target_hour_column"]]
        + list(spec["trace_columns"])
        + list(spec["categorical_features"])
        + list(spec["numeric_features"])
        + list(spec["binary_features"])
        + list(spec["missingness_indicators"])
    ))


def load_feature_dataset(spark: SparkSession, config: Mapping[str, Any]) -> DataFrame:
    """Load and validate the dedicated Task 1 ML dataset."""
    spec = _feature_spec(config)
    frame = spark.read.format("delta").load(spec["input_delta_path"])
    missing = sorted(set(feature_input_columns(config)) - set(frame.columns))
    if missing:
        raise ValueError(f"Task 1 ML dataset is missing feature columns: {missing}")
    return frame


def _binary_input_name(column: str) -> str:
    return f"__binary_{column}"


def _prepare_feature_inputs(dataset: DataFrame, config: Mapping[str, Any]) -> DataFrame:
    """Convert binary inputs to non-null doubles before vector assembly."""
    spec = _feature_spec(config)
    prepared = dataset
    for column in list(spec["binary_features"]) + list(spec["missingness_indicators"]):
        prepared = prepared.withColumn(
            _binary_input_name(column), F.coalesce(F.col(column).cast("double"), F.lit(0.0))
        )
    return prepared


def _validate_train_numeric_features(train: DataFrame, config: Mapping[str, Any]) -> None:
    """Reject numeric features with no observed training values before imputation."""
    numeric = list(_feature_spec(config)["numeric_features"])
    observed = train.agg(*[
        F.sum(
            F.when(F.col(column).isNotNull() & ~F.isnan(F.col(column)), F.lit(1))
            .otherwise(F.lit(0))
        ).alias(column)
        for column in numeric
    ]).first().asDict()
    unavailable = [column for column in numeric if not observed[column]]
    if unavailable:
        names = ", ".join(unavailable)
        raise ValueError(
            "Cannot fit numeric imputer: the train split has no observed values "
            f"for configured numeric feature(s): {names}. Provide train values, "
            "remove the feature, or define an explicit non-imputation policy."
        )


def build_feature_pipeline(config: Mapping[str, Any]) -> Pipeline:
    """Build the reusable categorical, imputation, scaling, and assembly pipeline."""
    spec = _feature_spec(config)
    categorical = list(spec["categorical_features"])
    numeric = list(spec["numeric_features"])
    binary = [
        _binary_input_name(column)
        for column in list(spec["binary_features"]) + list(spec["missingness_indicators"])
    ]
    index_columns = [f"__index_{column}" for column in categorical]
    encoded_columns = [f"__ohe_{column}" for column in categorical]
    imputed_columns = [f"__imputed_{column}" for column in numeric]

    stages = [
        StringIndexer(inputCol=column, outputCol=index, handleInvalid="keep")
        for column, index in zip(categorical, index_columns)
    ]
    stages.append(OneHotEncoder(
        inputCols=index_columns,
        outputCols=encoded_columns,
        handleInvalid="keep",
    ))
    stages.append(Imputer(
        inputCols=numeric,
        outputCols=imputed_columns,
        strategy=spec["imputer"]["strategy"],
    ))
    stages.append(VectorAssembler(inputCols=imputed_columns, outputCol="__numeric_vector"))

    scaling = spec["scaling"]
    numeric_vector = "__numeric_vector"
    if scaling["enabled"]:
        numeric_vector = "__scaled_numeric_vector"
        stages.append(StandardScaler(
            inputCol="__numeric_vector",
            outputCol=numeric_vector,
            withStd=bool(scaling.get("with_std", True)),
            withMean=bool(scaling.get("with_mean", False)),
        ))
    stages.append(VectorAssembler(
        inputCols=[numeric_vector, *encoded_columns, *binary], outputCol="features"
    ))
    return Pipeline(stages=stages)


def fit_transform_feature_dataset(
    dataset: DataFrame, config: Mapping[str, Any]
) -> FeatureTransformation:
    """Fit preprocessing on chronological train rows and transform every split."""
    spec = _feature_spec(config)
    prepared = _prepare_feature_inputs(dataset, config)
    train = prepared.filter(F.col(spec["split_column"]) == "train")
    if train.limit(1).count() == 0:
        raise ValueError("Feature pipeline requires at least one train row")
    _validate_train_numeric_features(train, config)
    model = build_feature_pipeline(config).fit(train)
    transformed = model.transform(prepared)
    model_ready = transformed.select(
        F.col(spec["label_column"]).cast("double").alias("label"),
        "features",
        F.col(spec["split_column"]).alias("split"),
        F.col(spec["target_hour_column"]).alias("target_hour"),
        *[F.col(column) for column in spec["trace_columns"]],
    )
    return FeatureTransformation(model=model, transformed=transformed, model_ready=model_ready)


def _feature_evidence(
    output: DataFrame, config: Mapping[str, Any]
) -> dict[str, Any]:
    """Summarize the materialized feature dataset without fitting an ML model."""
    spec = _feature_spec(config)
    total_rows = output.count()
    split_rows = {row["split"]: row["count"] for row in output.groupBy("split").count().collect()}
    null_counts = output.select(
        *[
            F.sum(F.col(column).isNull().cast("long")).alias(column)
            for column in output.columns
            if column != "features"
        ],
        F.sum(F.col("features").isNull().cast("long")).alias("features"),
    ).first().asDict()
    size = output.select(F.size(vector_to_array("features")).alias("size")).first()["size"]
    categorical = list(spec["categorical_features"])
    numeric = list(spec["numeric_features"])
    binary = list(spec["binary_features"])
    indicators = list(spec["missingness_indicators"])
    return {
        "input_delta_path": spec["input_delta_path"],
        "output_delta_path": spec["output_delta_path"],
        "categorical_features": categorical,
        "numeric_features": numeric,
        "binary_features": binary,
        "missingness_indicators": indicators,
        "imputation_strategy": spec["imputer"]["strategy"],
        "scaling_strategy": spec["scaling"],
        "fitted_on_train_only": True,
        "invalid_category_handling": "StringIndexer(handleInvalid=keep); OneHotEncoder(handleInvalid=keep)",
        "stages": [stage.__class__.__name__ for stage in build_feature_pipeline(config).getStages()],
        "final_feature_vector_size": size,
        "total_rows": total_rows,
        "split_rows": split_rows,
        "null_counts_after_transformation": null_counts,
        "final_output_schema": output.schema.jsonValue(),
    }


def materialize_feature_dataset(
    spark: SparkSession, config: Mapping[str, Any]
) -> dict[str, Any]:
    """Write model-ready features only to the dedicated Week 4 Delta path."""
    spec = _feature_spec(config)
    transformation = fit_transform_feature_dataset(load_feature_dataset(spark, config), config)
    output = transformation.model_ready.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        evidence = _feature_evidence(output, config)
        output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            spec["output_delta_path"]
        )
        evidence_path = Path(spec["evidence_path"])
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
        return evidence
    finally:
        output.unpersist()


def main() -> None:
    """Materialize the Task 2 model-ready feature dataset without training."""
    config = load_week4_config(DEFAULT_CONFIG_PATH)
    spark = get_spark_session(
        app_name="Week4FeatureEngineering", master="local[2]", shuffle_partitions=8
    )
    try:
        print(json.dumps(materialize_feature_dataset(spark, config), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
