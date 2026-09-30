# Week 4: Building Machine Learning Pipelines on the Urban Data Platform

## Overview

Week 4 extends the urban data platform developed during Weeks 1–3 into a reusable machine learning workflow using Spark MLlib.

The selected prediction task is **one-hour-ahead taxi demand forecasting at pickup-zone level**.

The implementation has four stages:

1. Training dataset generation
2. Reusable feature engineering
3. Reproducible model training and evaluation
4. Raw-data vs integrated-platform comparison

The emphasis is on reproducibility, reuse, and maintainable ML workflows rather than maximizing predictive accuracy.

---

# 1. Prediction Problem

## Objective

Predict the number of taxi pickups in each pickup zone during the next hour.

One row represents:

```text
pickup_location_id × target_hour
```

Target:

```text
next_hour_demand
```

Forecast horizon:

```text
1 hour ahead
```

For target hour `T`, the forecasting contract is:

```text
target                 = demand(T)
demand_lag_1h          = demand(T-1)
demand_lag_24h         = demand(T-24)
rolling_mean_24h       = mean[demand(T-24) ... demand(T-1)]
environmental context  = context(T-1)
```

Only information available before the target hour is used.

---

# 2. Data Sources

The platform integrates:

- Taxi trips
- Weather
- Air quality
- Taxi zone lookup

Primary integrated source:

```text
storage/delta/silver/integrated_taxi_trips
```

Relevant Delta versions:

- Version 0: original Jan–Mar 2024 baseline
- Version 1: later extended platform version

Week 4 uses:

```text
Main baseline ML experiment: version 0
Task 4 raw-vs-platform comparison: version 0
```

Version 0 is used so the integrated-platform workflow matches the original raw Jan–Mar 2024 source snapshot.

---

# 3. Project Structure

```text
config/
└── week4.yaml

src/week4/
├── __init__.py
├── dataset.py
├── features.py
├── evaluation.py
├── training.py
└── comparison.py

tests/
├── test_week4_dataset.py
├── test_week4_features.py
├── test_week4_evaluation.py
└── test_week4_comparison.py

storage/
├── delta/ml/
│   ├── hourly_taxi_demand/
│   └── hourly_taxi_demand_features/
└── metrics/week4/
    ├── task1_training_dataset.json
    ├── task2_feature_pipeline.json
    ├── task3_model_evaluation.json
    └── task4_platform_comparison.json
```

---

# 4. Environment Setup

Activate the project environment:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Use the project virtual environment for both Spark driver and workers:

```bash
export PYSPARK_PYTHON=.venv/bin/python
export PYSPARK_DRIVER_PYTHON=.venv/bin/python
export PYTHONPATH=.
```

`numpy` is required because PySpark ML imports it at runtime and is declared in `requirements.txt`.

---

# 5. Task 1: Training Dataset Generation

## Goal

Generate a reproducible ML dataset automatically from the integrated Delta platform.

Run:

```bash
PYSPARK_PYTHON=.venv/bin/python \
PYSPARK_DRIVER_PYTHON=.venv/bin/python \
PYTHONPATH=. \
.venv/bin/python -m src.week4.dataset
```

## Input

```text
storage/delta/silver/integrated_taxi_trips
```

Pinned source version:

```text
versionAsOf = 0
```

## Complete zone-hour grid

The trip-level source only contains zone-hours where at least one trip occurred. For demand forecasting, zero-demand observations are also valid.

Task 1 therefore constructs a complete:

```text
262 pickup zones × calendar hours
```

grid and fills absent observed demand with:

```text
demand = 0
```

This avoids training only on active zone-hours and makes lag construction reliable.

## Demand features

For target hour `T`:

```text
next_hour_demand = demand(T)
demand_lag_1h    = demand(T-1)
demand_lag_24h   = demand(T-24)
rolling_mean_24h = mean demand from T-24 through T-1
```

The first 24 hours per zone are excluded because a full 24-hour history is not yet available.

## Environmental context

Weather and PM2.5 features use context from:

```text
T-1
```

Weather is joined by hour.

PM2.5 is joined by borough-hour.

Realized target-hour weather and air quality are not used.

## Calendar features

Calendar attributes are derived from `target_hour`:

- hour
- day of week
- month
- weekend indicator

These are known before the target hour occurs.

## Leakage exclusions

The final feature contract excludes:

- trip identifiers
- current / target-hour realized demand as a feature
- dropoff datetime
- dropoff location/context
- trip duration
- trip distance
- fare amount
- total amount
- tip amount
- tolls
- fees / surcharges
- payment type
- other post-trip outcome fields

## Chronological split

No random split is used.

```text
Train:
2024-01-01 to 2024-02-15

Validation:
2024-02-16 to 2024-03-09

Test:
2024-03-10 to 2024-03-31
```

Splits are assigned by `target_hour`.

## Output

```text
storage/delta/ml/hourly_taxi_demand
```

Evidence:

```text
storage/metrics/week4/task1_training_dataset.json
```

## Results

```text
Total rows:        565,920
Train rows:        282,960
Validation rows:   144,624
Test rows:         138,336
Zero-demand share: ~58.18%
```

Integrity checks confirmed:

- one row per `pickup_location_id × target_hour`
- no duplicate keys
- zero-demand rows retained
- all zero-demand rows retain borough mapping
- weather and PM2.5 joins do not increase row count
- split overlap is zero
- leakage fields are absent
- target-relative lag semantics are correct

---

# 6. Task 2: Reusable Feature Engineering

## Goal

Transform the Task 1 dataset into a model-ready Spark ML representation.

Run:

```bash
PYSPARK_PYTHON=.venv/bin/python \
PYSPARK_DRIVER_PYTHON=.venv/bin/python \
PYTHONPATH=. \
.venv/bin/python -m src.week4.features
```

Implementation:

```text
src/week4/features.py
```

Reusable APIs:

```text
build_feature_pipeline
fit_transform_feature_dataset
materialize_feature_dataset
```

Feature definitions are config-driven through:

```text
config/week4.yaml
```

## Categorical features

```text
pickup_location_id
pickup_borough
hour
day_of_week
month
prior_hour_pickup_weather_code
prior_hour_pickup_pm25_source
```

Pipeline:

```text
StringIndexer
→ OneHotEncoder
```

Unknown categories use:

```text
handleInvalid = "keep"
```

## Numeric features

```text
demand_lag_1h
demand_lag_24h
rolling_mean_24h
prior_hour_pickup_temperature_c
prior_hour_pickup_humidity_pct
prior_hour_pickup_precipitation_mm
prior_hour_pickup_wind_speed_kmh
prior_hour_pickup_pm25
```

Pipeline:

```text
train-only median Imputer
→ numeric VectorAssembler
→ StandardScaler
```

Numeric preprocessing is fitted only on the training split.

Scaling is applied only to numeric features, not to one-hot encoded categorical features.

Scaling is config-driven so it can be disabled later.

## Binary and missingness features

Binary features include:

```text
is_weekend
prior_hour_pickup_weather_matched
```

Environmental missingness indicators are also preserved explicitly.

This avoids treating unknown environmental data as factual zero.

## All-null numeric guard

Before fitting Spark `Imputer`, the pipeline checks that every configured numeric feature has at least one observed training value.

If a feature is entirely null / NaN in the training split, the pipeline raises a clear `ValueError`.

It does not silently substitute zero.

## Final feature vector

| Feature group | Dimensions |
|---|---:|
| `pickup_location_id` one-hot | 263 |
| `pickup_borough` one-hot | 9 |
| `hour` one-hot | 25 |
| `day_of_week` one-hot | 8 |
| `month` one-hot | 3 |
| `prior_hour_pickup_weather_code` one-hot | 15 |
| `prior_hour_pickup_pm25_source` one-hot | 3 |
| **Categorical total** | **326** |
| Scaled numeric | 8 |
| Binary | 2 |
| Missingness indicators | 8 |
| **Total** | **344** |

## Final output columns

The model-ready output retains:

```text
label
features
split
target_hour
pickup_location_id
```

Traceability columns are preserved, but leakage-prone columns are not assembled into the model vector.

## Output

```text
storage/delta/ml/hourly_taxi_demand_features
```

Evidence:

```text
storage/metrics/week4/task2_feature_pipeline.json
```

## Results

```text
Rows preserved:          565,920
Train:                   282,960
Validation:              144,624
Test:                    138,336
Final feature dimension: 344
Null labels:             0
Null feature vectors:    0
```

---

# 7. Task 3: Reproducible ML Pipeline

## Goal

Train, compare, evaluate, persist, and reload multiple Spark ML regressors using the same feature representation and chronological data splits.

Run:

```bash
PYSPARK_PYTHON=.venv/bin/python \
PYSPARK_DRIVER_PYTHON=.venv/bin/python \
PYTHONPATH=. \
.venv/bin/python -m src.week4.training
```

## Baseline

A non-trained reference baseline is included:

```text
Naive lag-24h
```

It predicts using the demand observed at the same hour on the previous day.

## Candidate models

```text
Linear Regression
Poisson Generalized Linear Regression
Decision Tree Regressor
Random Forest Regressor
```

All candidates use:

- the same 344-dimensional Task 2 feature vector
- the same train split
- the same validation split
- the same test split
- the same preprocessing pipeline
- the same evaluation metrics

No hyperparameter sweep is performed.

## Fixed configurations

### Linear Regression

```text
maxIter = 100
regParam = 0.01
elasticNetParam = 0.0
```

### Poisson GLR

```text
family = "poisson"
link = "log"
maxIter = 100
regParam = 0.01
```

### Decision Tree

```text
maxDepth = 8
minInstancesPerNode = 5
```

### Random Forest

```text
numTrees = 20
maxDepth = 8
minInstancesPerNode = 5
seed = 42
```

## Evaluation metrics

```text
RMSE
MAE
R²
```

## Model selection

Selection uses validation data only.

Priority:

```text
1. Lowest validation RMSE
2. Lowest validation MAE
3. Highest validation R²
```

The held-out test split is not used during model selection.

## Validation results

| Model | RMSE | MAE | R² |
|---|---:|---:|---:|
| Naive lag-24h | 21.8598 | 5.6762 | 0.8304 |
| Linear Regression | **13.1759** | 4.9247 | **0.9384** |
| Poisson GLR | 40.2193 | 8.2373 | 0.4260 |
| Decision Tree | 16.0707 | **4.3595** | 0.9083 |
| Random Forest | 16.1656 | 4.4382 | 0.9073 |

## Selected model

```text
Linear Regression
```

It is selected because it has the lowest validation RMSE and highest validation R².

Decision Tree has the lowest MAE, but the predefined rule is RMSE-first.

## Held-out test result

```text
RMSE = 13.7418
MAE  = 5.1094
R²   = 0.9352
```

## Persistence and reproducibility

The preprocessing pipeline and fitted models are persisted to paths defined in:

```text
config/week4.yaml
```

Save/load verification for the selected model passed.

```text
Compared test predictions: 138,336
Maximum absolute difference: 0.0
```

Evidence:

```text
storage/metrics/week4/task3_model_evaluation.json
```

---

# 8. Task 4: Raw Data vs Integrated Platform

## Goal

Evaluate how the Weeks 1–3 data platform supports ML by comparing two workflows that produce the same Week 4 training input and fit the same selected model configuration.

No new model selection is performed in Task 4.

The fixed Linear Regression configuration from Task 3 is used for both approaches.

## Shared experiment contract

Both approaches use:

- the same Jan–Mar 2024 source period
- the same one-hour-ahead demand target
- the same complete zone-hour grid
- the same lag semantics
- the same rolling-demand semantics
- the same prior-hour environmental context
- the same chronological splits
- the same Task 2 feature configuration
- the same 344-dimensional representation
- the same Linear Regression configuration

## Approach A: Raw workflow

Inputs:

```text
Taxi Trips
Weather
Air Quality
Taxi Zone Lookup
```

Workflow:

```text
raw loading
→ casting / normalization
→ validation
→ duplicate / key handling
→ timestamp normalization
→ taxi-zone enrichment
→ weather integration
→ PM2.5 integration
→ Week 4 dataset generation
→ Task 2 feature preprocessing
→ fixed Linear Regression fit
```

Existing Weeks 1–3 ingestion and integration components are reused where possible.

The raw replay is isolated and does not write to production Bronze, Silver, Gold, quarantine, or metadata tables.

## Historical schema compatibility

The current Week 3 schema contract is stricter than the contract that existed when integrated Delta version 0 was originally created.

To reproduce the historical baseline fairly, Approach A retains:

- casting
- SQL validation
- key validation
- duplicate validation

but skips only the later batch-schema gate that would otherwise reject historical Weather columns that were valid when the baseline was built.

This is a backward-compatibility requirement for historical reproduction.

## Approach B: Integrated platform workflow

Input:

```text
storage/delta/silver/integrated_taxi_trips
```

Pinned version:

```text
versionAsOf = 0
```

Workflow:

```text
integrated Delta v0
→ Week 4 dataset generation
→ Task 2 feature preprocessing
→ fixed Linear Regression fit
```

This avoids repeating the raw ingestion, validation, normalization, and multi-source integration stages.

## Equivalence gate

Before runtime or model conclusions are accepted, both approaches must produce equivalent ML inputs.

The comparison checks:

- total rows
- distinct `pickup_location_id × target_hour` keys
- split assignments
- labels
- zero-demand share
- `demand_lag_1h`
- `demand_lag_24h`
- `rolling_mean_24h`
- environmental context
- categorical fields
- feature-vector dimension

Task 4 produced:

```text
565,920 rows in both workflows
344-dimensional feature vectors in both workflows
identical keys
identical splits
identical target labels
identical demand lags
identical zero-demand share
```

## PM2.5 precision tolerance

The only raw-vs-platform numeric drift was PM2.5.

Maximum absolute PM2.5 difference:

```text
0.010000000000000231
```

This corresponds to the integration contract's hundredth-level precision plus floating-point representation effects.

The comparison therefore uses:

```text
0.010001
```

as the explicit PM2.5 tolerance.

All keys, labels, splits, demand-history features, categories, and schema remain strictly checked.

Maximum feature-vector difference after preprocessing:

```text
0.0018800566
```

All target labels match exactly.

## Validation metrics

| Metric | Raw workflow | Integrated platform |
|---|---:|---:|
| RMSE | 13.17593634 | 13.17593627 |
| MAE | 4.92469411 | 4.92469348 |
| R² | 0.93839151 | 0.93839151 |

Predictive behavior is therefore effectively unchanged.

## End-to-end runtime

Measured locally:

```text
Raw sources → trained model:
353.785 s

Integrated Delta v0 → trained model:
104.719 s
```

These measurements describe the local experiment and should not be interpreted as production-cluster benchmarks.

The important result is that the integrated platform reduces the amount of work required before training.

It does not inherently make the Linear Regression estimator itself more accurate.

## Reproducibility

### Raw workflow

Reproduction depends on the same:

- raw files
- schemas
- casting logic
- validation logic
- timestamp normalization
- integration rules
- configuration

### Integrated workflow

Reproduction begins from:

```text
Delta versionAsOf = 0
```

which pins an already validated and enriched platform snapshot.

The raw workflow is reproducible, but it has a larger reproducibility surface because more preparation stages must be recreated.

## Production-table safety

Task 4 records production Delta versions before and after the experiment.

Result:

```text
21 production Delta table versions unchanged
0 production Delta writes
```

Evidence:

```text
storage/metrics/week4/task4_platform_comparison.json
```

---

# 9. Running the Complete Week 4 Workflow

Task 1:

```bash
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python PYTHONPATH=. \
.venv/bin/python -m src.week4.dataset
```

Task 2:

```bash
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python PYTHONPATH=. \
.venv/bin/python -m src.week4.features
```

Task 3:

```bash
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python PYTHONPATH=. \
.venv/bin/python -m src.week4.training
```

Task 4:

```bash
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python PYTHONPATH=. \
.venv/bin/python src/week4/comparison.py
```

---

# 10. Retraining

The pipeline supports retraining without changing the core architecture.

A retraining cycle is:

```text
new or updated integrated Delta snapshot
→ regenerate Task 1 dataset
→ reuse Task 2 feature configuration
→ fit preprocessing on the new train split
→ retrain configured candidate models
→ validate chronologically
→ evaluate the selected model on held-out test data
→ persist new preprocessing and model artifacts
```

Integrated Delta version 1 can be used for a future retraining / robustness experiment.

However, its post-March trip-volume distribution differs substantially from the original Jan–Mar baseline, so it should be interpreted carefully as a distribution-shift scenario rather than assumed to be a directly equivalent continuation.

---

# 11. Configuration

Main Week 4 configuration:

```text
config/week4.yaml
```

It defines:

- source Delta path
- source Delta version
- forecast horizon
- chronological split boundaries
- feature groups
- missing-value strategy
- scaling strategy
- leakage exclusions
- candidate models
- fixed model parameters
- output / artifact paths
- Task 4 comparison settings
- numeric comparison tolerances

This keeps Week 4 behavior configurable and reproducible.

---

# 12. Testing

Run all focused Week 4 tests:

```bash
PYSPARK_PYTHON=.venv/bin/python \
PYSPARK_DRIVER_PYTHON=.venv/bin/python \
PYTHONPATH=. \
.venv/bin/python -m unittest \
tests.test_week4_dataset \
tests.test_week4_features \
tests.test_week4_evaluation \
tests.test_week4_comparison -v
```

Final validation status:

```text
Week 4 focused tests: 17/17 passed
Python syntax validation: passed
git diff --check: passed
```

Coverage includes:

### Task 1

- target shift
- target-relative `lag_1h`
- target-relative `lag_24h`
- rolling-window causality
- zero-demand grid retention
- chronological splitting
- leakage exclusion
- deterministic schema / row behavior

### Task 2

- train-only imputation
- unseen-category handling
- numeric-null handling
- all-null numeric guard
- label / split preservation
- deterministic vector dimension
- leakage exclusion

### Task 3

- fixed candidate configuration
- validation-only model selection
- candidate evaluation contract
- persistence behavior

### Task 4

- common source snapshot
- output equivalence
- deliberate mismatch detection
- deterministic comparison metadata
- fixed Linear Regression configuration
- no production Delta writes
- production Delta version guard

---

# 13. Evidence Files

Machine-readable evidence is stored in:

```text
storage/metrics/week4/
```

Files:

```text
task1_training_dataset.json
task2_feature_pipeline.json
task3_model_evaluation.json
task4_platform_comparison.json
```

These contain the detailed counts, schemas, feature metadata, model metrics, runtime measurements, equivalence checks, and experiment settings used for the final reports.

---

# 14. Key Results Summary

## Training dataset

```text
Rows:              565,920
Train:             282,960
Validation:        144,624
Test:              138,336
Zero-demand share: ~58.18%
```

## Feature engineering

```text
Final vector:       344 dimensions
Categorical:        326
Scaled numeric:     8
Binary:             2
Missingness:        8
```

## Model validation

| Model | RMSE | MAE | R² |
|---|---:|---:|---:|
| Naive lag-24h | 21.8598 | 5.6762 | 0.8304 |
| Linear Regression | **13.1759** | 4.9247 | **0.9384** |
| Poisson GLR | 40.2193 | 8.2373 | 0.4260 |
| Decision Tree | 16.0707 | **4.3595** | 0.9083 |
| Random Forest | 16.1656 | 4.4382 | 0.9073 |

## Selected-model test result

```text
Linear Regression

RMSE = 13.7418
MAE  = 5.1094
R²   = 0.9352
```

## Task 4 comparison

```text
Raw sources → trained model:
353.785 s

Integrated Delta → trained model:
104.719 s
```

Both workflows produced:

```text
565,920 rows
344-dimensional feature vectors
identical target labels
near-identical Linear Regression validation metrics
```

---

# 15. Main Engineering Takeaway

The primary Week 4 result is not that the platform changes the estimator itself.

The Weeks 1–3 platform reduces the engineering required before model training.

The integrated workflow reuses:

- validated schemas
- cleaned datasets
- normalized timestamps
- taxi-zone enrichment
- weather integration
- PM2.5 integration
- versioned Delta snapshots

This allows the ML workflow to start much closer to feature engineering.

The raw workflow reaches effectively the same predictive result but requires substantially more preparation and integration work.

In the local experiment:

```text
Raw workflow:
353.785 s

Integrated workflow:
104.719 s
```

while Linear Regression validation performance remains effectively identical.

The platform therefore primarily provides:

```text
less repeated preprocessing
+ stronger reproducibility
+ reusable feature engineering
+ maintainable retraining
+ equivalent model inputs
+ equivalent predictive behavior
```

---

# 16. Known Limitations

## Zero-heavy target

Approximately 58% of targets are zero demand.

Metrics should therefore be interpreted together rather than relying on only one score.

## Linear Regression output domain

Taxi demand is non-negative, while unconstrained Linear Regression can theoretically produce negative predictions.

This is a known estimator limitation and is separate from the data-engineering objective of the assignment.

## Poisson GLR

Poisson GLR is conceptually suitable for count data but underperformed with the fixed Poisson/log configuration used here.

This does not imply Poisson regression is generally invalid; it means it did not fit this feature-target relationship as well as the alternatives in this experiment.

## Scaling and tree models

Numeric scaling is useful for linear models but not required by Decision Trees or Random Forests.

The same feature representation is intentionally reused across all models to keep the comparison consistent.

## Historical schema compatibility

The validation contract evolved after Delta version 0 was created.

Historical raw reproduction therefore requires compatibility handling for the newer batch-schema gate.

A future platform improvement would be to version validation contracts together with dataset/schema versions.

## Floating-point aggregation

Distributed PM2.5 aggregation produced a maximum raw-vs-platform difference at approximately the 0.01 precision unit.

Task 4 therefore uses a documented precision-aware tolerance rather than bit-for-bit equality for floating-point aggregates.

## Local runtime measurements

Task 4 runtimes were measured in the local Spark environment.

They demonstrate relative workflow cost in this experiment, not production-cluster performance.

## Extended Delta version 1

Version 1 contains a substantial temporal distribution shift after March 2024.

It is better treated as a future retraining / robustness / drift experiment than as the primary Week 4 baseline.

---

# 17. Possible Future Improvements

Potential extensions include:

- model drift monitoring
- feature drift monitoring
- model registry and model versioning
- an ML-specific Gold layer or feature store
- cyclical temporal encodings
- alternative count-regression models
- zero-inflated / hurdle models
- cluster-based runtime benchmarking
- versioned validation contracts
- additional urban datasets
- automated retraining triggers
