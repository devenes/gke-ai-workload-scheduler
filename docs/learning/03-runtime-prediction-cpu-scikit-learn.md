# Tutorial 03: Runtime Prediction Using CPU-Only `scikit-learn`

## 1. Learning Objectives

- Inspect the feature schema, bounds validation, and future-leakage guards in [`predictor/features.py`](../../predictor/features.py).
- Train the CPU-only `GradientBoostingRegressor` in [`predictor/train.py`](../../predictor/train.py) using separate training (`seed=202601`) and holdout (`seed=202602`) datasets.
- Evaluate holdout Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE) against global-median and per-class median baselines.
- Verify deterministic fallback behavior when the predictor service is unreachable (`T07`).

---

## 2. Pre-Execution Feature Schema & Leakage Prevention

The predictor accepts 7 raw pre-execution features and computes 4 nonlinear interaction terms (`17` total dimensions after one-hot encoding of the 7 workload classes):

- Raw features: `workload_class`, `input_size`, `cpu_request_milli`, `memory_request_mb`, `compute_intensity`, `memory_working_set_mb`, `historical_class_mean_sec`.
- Derived interaction features: `size_x_intensity`, `work_per_cpu_unit`, `memory_utilization_ratio`, `log_input_size`.
- Prohibited outcome fields (explicitly rejected with HTTP 400): `actual_duration_sec`, `completed_at`, `holdout_label`, `exit_code`.

---

## 3. Step-by-Step Hands-On Actions

### Step 1: Train the Predictor and Inspect Holdout Metrics

```bash
.venv/bin/python -m predictor.train --artifact-dir predictor/artifacts
```

Measured output from [`predictor/artifacts/metadata.json`](../../predictor/artifacts/metadata.json):
- **Holdout MAE**: `0.2443s` (vs Class-Median Baseline MAE `2.2836s` -> **`89.3%` MAE reduction**)
- **Holdout RMSE**: `0.3750s` (vs Class-Median Baseline RMSE `2.8639s` -> **`86.91%` RMSE reduction**)
- **Training Fingerprint (`seed=202601`)**: `b659dc5114320c787d9d176dc293d5fb5274559608a220876c3a5e88965fb373`
- **Holdout Fingerprint (`seed=202602`)**: `fc0892077e7eb1924a70427ea82e20d20a84fbd0082edc9ee2e9f5750f8ae962`

### Step 2: Run the Predictor Unit & HTTP Validation Suite

```bash
.venv/bin/pytest -v predictor/tests/test_predictor.py
```

### Step 3: Retrain via Bounded Kubernetes Job on GKE

```bash
kubectl apply -f deploy/jobs/retrain-predictor-job.yaml
kubectl -n ai-workload-scheduler logs job/predictor-retrain-job
```
