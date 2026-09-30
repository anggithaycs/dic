"""Small unit tests for validation-only regression-model selection."""

from __future__ import annotations

import unittest

from src.week4.dataset import load_week4_config
from src.week4.evaluation import select_validation_winner
from src.week4.training import _training_spec


class Week4EvaluationTests(unittest.TestCase):
    """Ensure validation metrics, rather than test results, determine selection."""

    def test_validation_rmse_selects_the_winner(self) -> None:
        winner = select_validation_winner({
            "linear_regression": {"rmse": 4.0, "mae": 3.0, "r2": 0.2},
            "decision_tree": {"rmse": 3.0, "mae": 2.9, "r2": 0.1},
            "random_forest": {"rmse": 3.5, "mae": 2.0, "r2": 0.9},
        })
        self.assertEqual(winner, "decision_tree")

    def test_mae_then_r2_break_equal_rmse_ties(self) -> None:
        winner = select_validation_winner({
            "linear_regression": {"rmse": 3.0, "mae": 2.0, "r2": 0.2},
            "decision_tree": {"rmse": 3.0, "mae": 2.0, "r2": 0.4},
            "random_forest": {"rmse": 3.0, "mae": 2.1, "r2": 0.9},
        })
        self.assertEqual(winner, "decision_tree")

    def test_poisson_glr_is_a_deterministic_configured_candidate(self) -> None:
        candidates = _training_spec(load_week4_config())["candidates"]
        self.assertEqual(
            set(candidates),
            {"linear_regression", "poisson_glr", "decision_tree", "random_forest"},
        )
        self.assertEqual(candidates["poisson_glr"], {
            "family": "poisson",
            "link": "log",
            "max_iter": 100,
            "reg_param": 0.01,
        })


if __name__ == "__main__":
    unittest.main(verbosity=2)
