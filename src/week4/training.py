"""Fixed-candidate Week 4 regression training and validation-only selection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GeneralizedLinearRegression,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_session import get_spark_session, stop_spark_session
from src.week4.dataset import DEFAULT_CONFIG_PATH, load_week4_config
from src.week4.evaluation import regression_metrics, select_validation_winner
from src.week4.features import (
    _prepare_feature_inputs,
    fit_transform_feature_dataset,
    load_feature_dataset,
)


def _training_spec(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the fixed Task 3 model-comparison configuration."""
    spec = config.get("model_training")
    if not isinstance(spec, Mapping):
        raise ValueError("Week 4 config: model_training is required for Task 3")
    required = {"model_root_path", "evidence_path", "random_seed", "naive_reference", "candidates"}
    missing = sorted(required - set(spec))
    if missing:
        raise ValueError(f"Week 4 model_training: missing {missing}")
    expected = {"linear_regression", "poisson_glr", "decision_tree", "random_forest"}
    if set(spec["candidates"]) != expected:
        raise ValueError(f"Week 4 model_training candidates must be exactly {sorted(expected)}")
    return spec


def build_candidate_regressors(config: Mapping[str, Any]) -> dict[str, Any]:
    """Build the documented, deliberately small set of comparison models."""
    spec = _training_spec(config)
    candidates = spec["candidates"]
    seed = int(spec["random_seed"])
    return {
        "linear_regression": LinearRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=int(candidates["linear_regression"]["max_iter"]),
            regParam=float(candidates["linear_regression"]["reg_param"]),
            elasticNetParam=float(candidates["linear_regression"]["elastic_net_param"]),
        ),
        "poisson_glr": GeneralizedLinearRegression(
            featuresCol="features",
            labelCol="label",
            family=str(candidates["poisson_glr"]["family"]),
            link=str(candidates["poisson_glr"]["link"]),
            maxIter=int(candidates["poisson_glr"]["max_iter"]),
            regParam=float(candidates["poisson_glr"]["reg_param"]),
        ),
        "decision_tree": DecisionTreeRegressor(
            featuresCol="features",
            labelCol="label",
            maxDepth=int(candidates["decision_tree"]["max_depth"]),
            minInstancesPerNode=int(candidates["decision_tree"]["min_instances_per_node"]),
            seed=seed,
        ),
        "random_forest": RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            numTrees=int(candidates["random_forest"]["num_trees"]),
            maxDepth=int(candidates["random_forest"]["max_depth"]),
            minInstancesPerNode=int(candidates["random_forest"]["min_instances_per_node"]),
            featureSubsetStrategy=str(candidates["random_forest"]["feature_subset_strategy"]),
            seed=seed,
        ),
    }


def _split(frame: DataFrame, name: str) -> DataFrame:
    """Select one preserved chronological split."""
    return frame.filter(F.col("split") == name)


def _naive_predictions(
    frame: DataFrame, lag_column: str, label_column: str
) -> DataFrame:
    """Create the lag-24-hour reference prediction without model fitting."""
    return frame.select(
        F.col(label_column).cast("double").alias("label"),
        F.col(lag_column).cast("double").alias("prediction"),
    )


def _save_model(model: Any, path: Path) -> None:
    """Overwrite only the dedicated Week 4 model location."""
    model.write().overwrite().save(str(path))


def _prediction_round_trip_check(
    preprocessor: PipelineModel,
    selected_model: Any,
    prepared_test: DataFrame,
    original_predictions: DataFrame,
    config: Mapping[str, Any],
    model_path: Path,
    preprocessor_path: Path,
) -> dict[str, Any]:
    """Reload saved artifacts and verify selected-model predictions are identical."""
    loaded_preprocessor = PipelineModel.load(str(preprocessor_path))
    loaded_model = selected_model.__class__.load(str(model_path))
    reloaded = loaded_model.transform(loaded_preprocessor.transform(prepared_test)).select(
        "pickup_location_id", "target_hour", F.col("prediction").alias("reloaded_prediction")
    )
    original = original_predictions.select(
        "pickup_location_id", "target_hour", F.col("prediction").alias("original_prediction")
    )
    comparison = original.join(reloaded, ["pickup_location_id", "target_hour"], "inner")
    result = comparison.agg(
        F.count("*").alias("compared_rows"),
        F.max(F.abs(F.col("original_prediction") - F.col("reloaded_prediction"))).alias("max_abs_difference"),
    ).first().asDict()
    expected_rows = original.count()
    return {
        "passed": result["compared_rows"] == expected_rows and (result["max_abs_difference"] or 0.0) == 0.0,
        "compared_rows": result["compared_rows"],
        "max_abs_difference": result["max_abs_difference"] or 0.0,
    }


def run_model_comparison(spark: SparkSession, config: Mapping[str, Any]) -> dict[str, Any]:
    """Fit candidates, select from validation only, then test the selected model."""
    training = _training_spec(config)
    features = config["feature_pipeline"]
    raw = load_feature_dataset(spark, config)
    transformation = fit_transform_feature_dataset(raw, config)
    train = _split(transformation.model_ready, "train").cache()
    validation = _split(transformation.model_ready, "validation").cache()
    test = _split(transformation.model_ready, "test").cache()
    prepared_test = _prepare_feature_inputs(raw, config).filter(
        F.col(features["split_column"]) == "test"
    )
    root = Path(training["model_root_path"])
    preprocessor_path = root / "preprocessing_pipeline"
    try:
        _save_model(transformation.model, preprocessor_path)
        validation_results = {
            "naive_lag_24h": regression_metrics(
                _naive_predictions(
                    transformation.transformed.filter(F.col(features["split_column"]) == "validation"),
                    training["naive_reference"]["prediction_column"],
                    features["label_column"],
                )
            )
        }
        fitted_models: dict[str, Any] = {}
        validation_predictions: dict[str, DataFrame] = {}
        for name, estimator in build_candidate_regressors(config).items():
            model = estimator.fit(train)
            model_path = root / name
            _save_model(model, model_path)
            fitted_models[name] = model
            prediction = model.transform(validation)
            validation_predictions[name] = prediction
            validation_results[name] = regression_metrics(prediction)

        candidate_results = {name: validation_results[name] for name in fitted_models}
        selected_name = select_validation_winner(candidate_results)
        selected_model = fitted_models[selected_name]
        selected_path = root / selected_name
        selected_test_predictions = selected_model.transform(test)
        selected_test_metrics = regression_metrics(selected_test_predictions)
        round_trip = _prediction_round_trip_check(
            transformation.model,
            selected_model,
            prepared_test,
            selected_test_predictions,
            config,
            selected_path,
            preprocessor_path,
        )
        if not round_trip["passed"]:
            raise RuntimeError("Saved preprocessing/model artifacts changed test predictions")

        evidence = {
            "input_delta_path": features["input_delta_path"],
            "feature_vector_size": transformation.model_ready.select(
                F.size(vector_to_array("features")).alias("size")
            ).first()["size"],
            "split_rows": {"train": train.count(), "validation": validation.count(), "test": test.count()},
            "candidate_configurations": training["candidates"],
            "naive_reference": training["naive_reference"],
            "validation_metrics": validation_results,
            "selection_basis": "Validation RMSE, then validation MAE, then higher validation R2",
            "selected_model": selected_name,
            "selected_model_test_metrics": selected_test_metrics,
            "saved_artifacts": {
                "preprocessing_pipeline": str(preprocessor_path),
                "candidate_models": {name: str(root / name) for name in fitted_models},
            },
            "save_load_prediction_check": round_trip,
            "test_selection_used": False,
        }
        evidence_path = Path(training["evidence_path"])
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
        return evidence
    finally:
        train.unpersist()
        validation.unpersist()
        test.unpersist()


def main() -> None:
    """Run Task 3's fixed model comparison without hyperparameter search."""
    config = load_week4_config(DEFAULT_CONFIG_PATH)
    spark = get_spark_session(
        app_name="Week4ModelComparison", master="local[2]", shuffle_partitions=8
    )
    try:
        print(json.dumps(run_model_comparison(spark, config), indent=2))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
