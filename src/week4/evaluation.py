"""Shared regression evaluation and validation-only model selection helpers."""

from __future__ import annotations

from typing import Mapping

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame


METRICS = ("rmse", "mae", "r2")


def regression_metrics(frame: DataFrame, label_col: str = "label") -> dict[str, float]:
    """Calculate the common metrics from a label/prediction DataFrame."""
    return {
        metric: float(
            RegressionEvaluator(
                labelCol=label_col, predictionCol="prediction", metricName=metric
            ).evaluate(frame)
        )
        for metric in METRICS
    }


def select_validation_winner(
    candidate_metrics: Mapping[str, Mapping[str, float]]
) -> str:
    """Select the ML candidate by validation RMSE, then MAE, then higher R²."""
    if not candidate_metrics:
        raise ValueError("At least one candidate validation result is required")
    return min(
        candidate_metrics,
        key=lambda name: (
            candidate_metrics[name]["rmse"],
            candidate_metrics[name]["mae"],
            -candidate_metrics[name]["r2"],
            name,
        ),
    )
