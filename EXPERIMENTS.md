# Experimental Design, Benchmarks, and Verification Evidence (`EXPERIMENTS.md`)

**Project:** `gke-ai-workload-scheduler`  
**Target Environment:** `your-gcp-project-id` (`us-west4-a`), GKE Standard cluster `gke-ai-workload-scheduler` (`1 x e2-standard-4`, `4 vCPU`, `16 GiB RAM`)  
**Full Evidence Suite Path:** [`artifacts/test-results/run-20261024T052934Z/summary.json`](artifacts/test-results/run-20261024T052934Z/summary.json) (`20/20 PASS`)  
**Reproducibility Rerun Path:** [`artifacts/test-results/repro-20261024T053407Z/summary.json`](artifacts/test-results/repro-20261024T053407Z/summary.json) (`7/7 PASS`)  
**Initial Diagnostic Run Path (Preserved):** [`artifacts/test-results/run-20261024T052347Z/summary.json`](artifacts/test-results/run-20261024T052347Z/summary.json) (`19/20 PASS`, `T04` diagnosed & fixed)  
**Progressive Live GKE Scaling Evidence:** [`artifacts/benchmarks/progressive_live_gke_scaling.json`](artifacts/benchmarks/progressive_live_gke_scaling.json) (`n=5, 10` at `concurrency=4`)

---

## 1. Predeclared Hypotheses and Verification Outcomes

Before running policy comparisons, three quantitative hypotheses (`H1`, `H2`, `H3`) were predeclared in [`internal/scheduler/engine.go`](internal/scheduler/engine.go) (`EvaluateHypotheses`) and verified in [`T08.json`](artifacts/test-results/run-20261024T052934Z/T08.json) and [`T12.json`](artifacts/test-results/run-20261024T052934Z/T12.json):

| Hypothesis ID | Predeclared Statement | Target Metric | Baseline Value | Measured Value | Improvement / Margin | Threshold | Supported? |
|---|---|---|---|---|---|---|---|
| **H1 (Predictor Accuracy)** | The CPU `scikit-learn` `GradientBoostingRegressor` (`v1.0.0-cpu-gbr`) reduces runtime estimation MAE by at least `25%` compared with the per-class historical baseline on both holdout and evaluated benchmark workloads. | `prediction_mae_sec` | Holdout: `2.2836s` (`class_median`)<br>Sim (`n=25`): `2.6886s`<br>Live GKE (`T12`): `1.9667s` | Holdout: `0.2443s`<br>Sim (`n=25`): `0.1595s`<br>Live GKE (`T12`): `0.3206s` | Holdout: **`89.30%`**<br>Sim (`n=25`): **`94.07%`**<br>Live GKE (`T12`): **`83.70%`** | `>= 25.0%` | **YES (`PASS`)** |
| **H2 (AI-Assisted Scheduling Efficiency)** | Policy E (`ai_assisted`) improves mean queue waiting time or deadline attainment rate by at least `5%` over Policy A (`fifo`) on mixed and deadline-sensitive workloads while balancing throughput (`SJF`) and urgency (`EDF`). | `mean_queue_wait_sec` & `deadline_success_rate_pct` | Sim (`n=25`): `12.5677s` wait, `28.00%` deadlines<br>Sim (`n=50`): `22.2910s` wait, `33.33%` deadlines<br>Live GKE (`T12`): `4.1086s` wait, `66.67%` deadlines | Sim (`n=25`): `10.9633s` wait, `76.00%` deadlines<br>Sim (`n=50`): `14.9954s` wait, `100.00%` deadlines<br>Live GKE (`T12`): `3.8045s` wait, `83.33%` deadlines | Sim (`n=25`): **`12.77%`** wait reduction (`+48.0 pp` deadlines)<br>Sim (`n=50`): **`32.73%`** wait reduction (`+66.7 pp` deadlines)<br>Live GKE: **`7.40%`** wait reduction (`+16.7 pp` deadlines) | `>= 5.0%` | **YES (`PASS`)** |
| **H3 (Safety, Admission Gate & Anti-Starvation)** | Policy E (`ai_assisted`) strictly obeys `MaxConcurrentJobs`, CPU/memory resource budgets, and anti-starvation bounds (`0` safety violations across all runs). | `safety_violations_count` | `0` allowed violations | `0` violations (`peak_active_jobs <= concurrency`, `peak_cpu <= budget`, `starvation_detected = false`) | **`100%` compliance** | `0` violations | **YES (`PASS`)** |

---

## 2. CPU-Only Runtime Predictor Evaluation (`T02`, `T12`)

The runtime predictor ([`predictor/train.py`](predictor/train.py), [`predictor/features.py`](predictor/features.py)) uses a CPU-only `scikit-learn` (`1.5.2`) `GradientBoostingRegressor` (`n_estimators=180`, `learning_rate=0.06`, `max_depth=4`, `min_samples_leaf=5`, `subsample=0.9`, `random_state=202601`).

### 2.1 Dataset Separation & Anti-Leakage Verification
- **Training Dataset (`seed=202601`, `1,260` samples across 7 classes):** SHA-256 fingerprint `b659dc5114320c787d9d176dc293d5fb5274559608a220876c3a5e88965fb373`
- **Holdout Dataset (`seed=202602`, `350` samples across 7 classes):** SHA-256 fingerprint `fc0892077e7eb1924a70427ea82e20d20a84fbd0082edc9ee2e9f5750f8ae962`
- **Model Artifact ([`predictor/artifacts/model.joblib`](predictor/artifacts/model.joblib)):** SHA-256 `b33eed33af3bce66cf0ee8b762f777f241c8b1326817210879260f6cfbda0219`
- **Prohibited Post-Completion Fields Rejected by Schema Validator:** `actual_duration_sec`, `completed_at`, `holdout_label`, `exit_code`.

### 2.2 Holdout Evaluation vs. Naive Baselines (`n = 350`)

Measured from [`predictor/artifacts/holdout_evaluation.json`](predictor/artifacts/holdout_evaluation.json) and verified in [`T02.json`](artifacts/test-results/run-20261024T052934Z/T02.json):

| Estimator | Holdout MAE (s) | Holdout RMSE (s) | $R^2$ Score | p95 Abs Error (s) | Max Abs Error (s) | MAE Reduction vs. Class Median |
|---|---:|---:|---:|---:|---:|---:|
| **Global Median Baseline (`4.9726s`)** | `3.6812` | `5.3400` | `-0.1036` | `10.5617` | `20.0274` | `-61.20%` |
| **Per-Class Median Baseline** | `2.2836` | `3.8290` | `0.4326` | `8.1322` | `17.7696` | `0.00%` (Reference) |
| **ML Predictor (`v1.0.0-cpu-gbr`)** | **`0.2443`** | **`0.3750`** | **`0.9946`** | **`0.5874`** | **`3.6289`** | **`+89.30%` (`9.35x` lower MAE)** |

### 2.3 Top Feature Importances ([`predictor/artifacts/metadata.json`](predictor/artifacts/metadata.json))
1. `work_per_cpu_unit` (`(input_size * compute_intensity) / (cpu_request_milli / 1000.0)`): **`0.6532`** (`65.32%`)
2. `historical_class_mean_sec`: **`0.1893`** (`18.93%`)
3. `size_x_intensity` (`input_size * compute_intensity`): **`0.0771`** (`7.71%`)
4. `memory_working_set_mb`: **`0.0308`** (`3.08%`)
5. `class_long_cpu`: **`0.0243`** (`2.43%`)
6. `class_memory_sensitive`: **`0.0123`** (`1.23%`)

---

## 3. Offline Deterministic 5-Policy Simulation Benchmarks (`T08`, `T14`)

All 5 policies (`fifo`, `sjf`, `edf`, `priority_aging`, `ai_assisted`) were evaluated on identical seeded task batches (`seed=42`, `concurrency=4`, `cpu_budget=2000m`, `memory_budget=4096Mi`) in [`T08-offline-policy-comparisons.json`](artifacts/test-results/run-20261024T052934Z/logs/T08-offline-policy-comparisons.json).

> **Statistical Note on Percentiles:** For `n = 25`, `p99` is reported alongside sample size (`n = 25`) with full `p50`/`p95`/`p99` statistical stability established in the `n = 50` and `n = 100` runs below.

### 3.1 Scenario 1: `deadline_sensitive` (`n = 25`, `seed = 42`, `concurrency = 4`)
- **Batch SHA-256 Fingerprint:** `8d61d0faf56696d493bd933c681977c309f78fb322a3b89e4857c6b73b71a095`

| Policy | Makespan (s) | Mean Queue Wait (s) | p50 Wait (s) | p95 Wait (s) | p99 Wait (s) | Deadline Attainment | Jain's Fairness | Starvation? |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| **Policy E: `ai_assisted`** | `30.278` | `10.963` (`-12.77%` vs FIFO) | **`8.549`** | `22.481` | `23.816` | **`76.00%` (`19/25`)** | `0.6674` | `false` |
| **Policy C: `edf`** | **`28.655`** | `11.143` (`-11.34%` vs FIFO) | `9.099` | `25.145` | `26.787` | `72.00%` (`18/25`) | `0.6688` | `false` |
| **Policy B: `sjf`** | `30.288` | **`9.965`** (`-20.71%` vs FIFO) | `8.962` | **`18.995`** | **`20.154`** | `52.00%` (`13/25`) | **`0.7010`** | `false` |
| **Policy D: `priority_aging`** | `29.953` | `11.735` (`-6.62%` vs FIFO) | `9.310` | `21.011` | `23.490` | `48.00%` (`12/25`) | `0.6364` | `false` |
| **Policy A: `fifo`** | `30.922` | `12.568` (Baseline) | `12.320` | `21.933` | `22.747` | `28.00%` (`7/25`) | `0.5881` | `false` |

**Analysis (`n = 25`, `deadline_sensitive`):**
- Pure `fifo` suffers head-of-line blocking behind `long_cpu` and high-intensity tasks, missing `18/25` deadlines (`28.00%` success).
- Pure `sjf` achieves the lowest mean queue wait (`9.965s`), but ignores deadline slack and misses `12/25` deadlines (`52.00%` success).
- Pure `edf` prioritizes urgent deadlines (`72.00%` success), but occasionally schedules a long urgent task ahead of two short urgent tasks, causing both short tasks to miss their deadlines.
- **`ai_assisted` (`76.00%` deadline attainment, `19/25`)** combines ML runtime cost (`W_runtime = 1.0`) with continuous deadline slack urgency (`W_deadline = 2.6`), dispatching short urgent tasks ahead of long urgent tasks when both are at risk, achieving the **highest deadline attainment (`76.00%`)** and **lowest median wait (`p50 = 8.549s`)** of all 5 policies.

### 3.2 Scenario 2: `mixed` (`n = 50`, `seed = 42`, `concurrency = 4`)
- **Batch SHA-256 Fingerprint:** `d39727933810c750d722f11fa07290009df7e18a6035b7e39de9b62f02d7ae5e`

| Policy | Makespan (s) | Mean Queue Wait (s) | p50 Wait (s) | p95 Wait (s) | p99 Wait (s) | Deadline Attainment | Jain's Fairness | Starvation? |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| **Policy E: `ai_assisted`** | **`58.867`** | `14.995` (`-32.73%` vs FIFO) | `11.299` | `40.319` | **`42.105`** | **`100.00%` (`30/30`)** | `0.6466` | `false` |
| **Policy C: `edf`** | `59.375` | `16.796` (`-24.65%` vs FIFO) | `13.878` | `38.902` | `43.482` | **`100.00%` (`30/30`)** | `0.6108` | `false` |
| **Policy B: `sjf`** | `61.571` | **`12.192`** (`-45.31%` vs FIFO) | **`8.627`** | **`36.625`** | `44.809` | `76.67%` (`23/30`) | **`0.7468`** | `false` |
| **Policy D: `priority_aging`** | `58.992` | `17.625` (`-20.93%` vs FIFO) | `15.211` | `41.397` | `43.099` | `66.67%` (`20/30`) | `0.5710` | `false` |
| **Policy A: `fifo`** | `62.308` | `22.291` (Baseline) | `21.996` | `45.021` | `46.415` | `33.33%` (`10/30`) | `0.4821` | `false` |

### 3.3 Scenario 3: `mixed` (`n = 100`, `seed = 42`, `concurrency = 4`)
- **Batch SHA-256 Fingerprint:** `50f31ea226ba199483155886391db516993780e38b4a594f13711544db2b507f`

| Policy | Makespan (s) | Mean Queue Wait (s) | p50 Wait (s) | p95 Wait (s) | p99 Wait (s) | Deadline Attainment | Jain's Fairness | Starvation? |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| **Policy E: `ai_assisted`** | `115.904` | `32.697` (`-31.29%` vs FIFO) | `27.737` | `79.812` | **`92.367`** | `98.15%` (`53/54`) | `0.4791` | `false` |
| **Policy C: `edf`** | **`111.055`** | `39.217` (`-17.59%` vs FIFO) | `32.024` | `91.166` | `92.997` | **`100.00%` (`54/54`)** | `0.4418` | `false` |
| **Policy B: `sjf`** | `114.608` | **`25.464`** (`-46.49%` vs FIFO) | **`17.200`** | **`78.963`** | `93.606` | `68.52%` (`37/54`) | **`0.5285`** | `false` |
| **Policy D: `priority_aging`** | `114.179` | `39.686` (`-16.60%` vs FIFO) | `33.876` | `88.416` | `96.711` | `33.33%` (`18/54`) | `0.4229` | `false` |
| **Policy A: `fifo`** | `111.217` | `47.585` (Baseline) | `46.415` | `93.545` | `97.206` | `18.52%` (`10/54`) | `0.3490` | `false` |

---

## 4. Live GKE `batch/v1` Job Benchmarks (`T10`, `T11`, `T12`, Progressive Scaling)

### 4.1 Live GKE 5-Policy Comparison (`T12`, `deadline_sensitive`, `seed = 108`, `n = 6`, `concurrency = 3`)

Executed as real Kubernetes `batch/v1` `Job` pods (`/app/scheduler-worker`) on node `gke-gke-ai-workload-s-cpu-worker-pool-52db7b80-003q` (`e2-standard-4` in `us-west4-a`). Full per-task evidence is stored in [`T12.json`](artifacts/test-results/run-20261024T052934Z/T12.json) and [`T12-live-5policy-comparison.json`](artifacts/test-results/run-20261024T052934Z/logs/T12-live-5policy-comparison.json):

| Policy (Live GKE `batch/v1` Jobs) | Live Makespan (s) | Mean Queue Wait (s) | Deadline Attainment | Live Prediction MAE (s) | Historical Baseline MAE (s) |
|---|---:|---:|---:|---:|---:|
| **Policy E: `ai_assisted`** | `13.5359` | **`3.8045`** (`-7.40%` vs FIFO, `-15.07%` vs EDF) | **`83.33%` (`5/6`)** | **`0.3206`** | `1.9667` |
| **Policy B: `sjf`** | `13.7158` | `3.4926` (`-14.99%` vs FIFO) | **`83.33%` (`5/6`)** | `0.3788` | `1.9667` |
| **Policy D: `priority_aging`** | `15.3339` | `4.3720` | **`83.33%` (`5/6`)** | `0.3206` | `1.9667` |
| **Policy C: `edf`** | `16.2915` | `4.4797` | **`83.33%` (`5/6`)** | `0.5359` | `1.9667` |
| **Policy A: `fifo`** | `12.1021` | `4.1086` | `66.67%` (`4/6`) | `0.4455` | `1.9667` |

### 4.2 Per-Task Predicted vs. Actual Container Runtime on Live GKE (`T12` `ai_assisted` Run)

| Task ID | Kubernetes Job Name | Predicted Duration (s) | Actual GKE Container Duration (s) | Absolute Error (s) | Historical Class Mean (s) |
|---|---|---:|---:|---:|---:|
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-001` | `ws-ne-sensitive-s108-n6-c3-9950-001` | `5.7889` | `5.0000` | `0.7889` | `3.9000` |
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-002` | `ws-ne-sensitive-s108-n6-c3-9950-002` | `1.6747` | `2.0000` | `0.3253` | `3.9000` |
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-003` | `ws-ne-sensitive-s108-n6-c3-9950-003` | `1.7404` | `2.0000` | `0.2596` | `3.9000` |
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-004` | `ws-ne-sensitive-s108-n6-c3-9950-004` | `6.7001` | `7.0000` | `0.2999` | `3.9000` |
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-005` | `ws-ne-sensitive-s108-n6-c3-9950-005` | `1.7710` | `2.0000` | `0.2290` | `3.9000` |
| `gke-ai_assisted-deadline_sensitive-s108-n6-c3-9950-006` | `ws-ne-sensitive-s108-n6-c3-9950-006` | `1.9792` | `2.0000` | `0.0208` | `3.9000` |
| **Summary** | **6/6 Succeeded (`exit_code=0`)** | **Mean: `3.2757s`** | **Mean: `3.3333s`** | **MAE: `0.3206s` (RMSE: `0.3955s`)** | **Baseline MAE: `1.9667s`** |

### 4.3 Live GKE Concurrency & Resource Budget Gate (`T11`)

Measured from [`T11.json`](artifacts/test-results/run-20261024T052934Z/T11.json):
- **`concurrency = 2`, `cpu_budget = 600m` (`n = 5` tasks):** Peak active Kubernetes Jobs = **`2`** (`<= 2`), Peak requested CPU = **`550m`** (`<= 600m`), Makespan = `8.93s`.
- **`concurrency = 4`, `cpu_budget = 1400m` (`n = 8` tasks):** Peak active Kubernetes Jobs = **`4`** (`<= 4`), Peak requested CPU = **`1050m`** (`<= 1400m`), Makespan = `8.82s`.

### 4.4 Progressive Workload Scaling Across Live GKE (`n = 5, 6, 8, 10`) and Simulation (`n = 25, 50, 100`)

Measured from [`artifacts/benchmarks/progressive_live_gke_scaling.json`](artifacts/benchmarks/progressive_live_gke_scaling.json), [`T10.json`](artifacts/test-results/run-20261024T052934Z/T10.json), [`T11.json`](artifacts/test-results/run-20261024T052934Z/T11.json), and [`T08.json`](artifacts/test-results/run-20261024T052934Z/T08.json):

| Workload Size (`n`) | Execution Mode | Scenario & Seed | Concurrency Cap (`c`) | Peak Active Jobs | Makespan (s) | Mean Queue Wait (s) | Prediction MAE (s) | Succeeded / Submitted |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| **`n = 5`** | **Live GKE `batch/v1` Jobs** | `short_cpu` (`seed=42`) | `4` | `4` | `8.5402` | `4.1845` | `0.5129` | `5 / 5` (`100%`) |
| **`n = 6`** | **Live GKE `batch/v1` Jobs** | `deadline_sensitive` (`seed=108`) | `3` | `3` | `13.5359` | `3.8045` | `0.3206` | `6 / 6` (`100%`) |
| **`n = 8`** | **Live GKE `batch/v1` Jobs** | `short_cpu` (`seed=77`) | `4` | `4` | `8.8158` | `4.5120` | `0.4810` | `8 / 8` (`100%`) |
| **`n = 10`** | **Live GKE `batch/v1` Jobs** | `short_cpu` (`seed=42`) | `4` | `4` | `19.8860` | `9.8328` | `0.4529` | `10 / 10` (`100%`) |
| **`n = 25`** | **Deterministic Simulation** | `deadline_sensitive` (`seed=42`) | `4` | `4` | `30.2783` | `10.9633` | `0.1595` | `25 / 25` (`100%`) |
| **`n = 50`** | **Deterministic Simulation** | `mixed` (`seed=42`) | `4` | `4` | `58.8669` | `14.9954` | `0.1232` | `50 / 50` (`100%`) |
| **`n = 100`** | **Deterministic Simulation** | `mixed` (`seed=42`) | `4` | `4` | `115.9040` | `32.6966` | `0.1251` | `100 / 100` (`100%`) |

---

## 5. Complete `T00`–`T19` Verification Matrix (`20/20 PASS`)

Every test in [`scripts/run_test_suite.py`](scripts/run_test_suite.py) produces a self-contained JSON evidence record under [`artifacts/test-results/run-20261024T052934Z/`](artifacts/test-results/run-20261024T052934Z/):

| Test ID | Verification Scope | Status | Execution | Key Measured Outcome | Evidence File |
|---|---|---|---|---|---|
| **`T00`** | Environment, GKE Cluster, Node Capacity & API Preflight | **`PASS`** | `COMPLETED` | Cluster `RUNNING` (`1.36.4-gke.1495002`), `1` Ready node (`3920m` allocatable CPU), `overview` HTTP `200` | [`T00.json`](artifacts/test-results/run-20261024T052934Z/T00.json) |
| **`T01`** | Deterministic Workload Generator & Schema Validation | **`PASS`** | `COMPLETED` | Go unit tests `PASS`; `seed=42` produces identical SHA-256 fingerprint across runs | [`T01.json`](artifacts/test-results/run-20261024T052934Z/T01.json) |
| **`T02`** | CPU Runtime Predictor Holdout Accuracy & Anti-Leakage | **`PASS`** | `COMPLETED` | Holdout MAE `0.2443s` vs class median `2.2836s` (`89.30%` reduction); `actual_duration_sec` rejected (`400`) | [`T02.json`](artifacts/test-results/run-20261024T052934Z/T02.json) |
| **`T03`** | Policy A (`fifo`) Strict Arrival Ordering | **`PASS`** | `COMPLETED` | `20/20` tasks admitted strictly in `arrival_sequence` `1..20` (`dispatch_order_matches_admission=true`) | [`T03.json`](artifacts/test-results/run-20261024T052934Z/T03.json) |
| **`T04`** | Policy B (`sjf`) Pre-Execution Prediction Monotonicity | **`PASS`** | `COMPLETED` | All `19` queued tasks ordered monotonically by `predicted_duration_sec` (`1.3671s <= 1.5117s <= ...`) | [`T04.json`](artifacts/test-results/run-20261024T052934Z/T04.json) |
| **`T05`** | Policy C (`edf`) Three-Tier Deadline & Slack Ordering | **`PASS`** | `COMPLETED` | Urgent viable (`6s`) admitted `#1`, later viable (`25s`) `#2`, no-deadline `#3`, expired deadline `#4` | [`T05.json`](artifacts/test-results/run-20261024T052934Z/T05.json) |
| **`T06`** | Policy D (`priority_aging`) Bounded Aging Anti-Starvation | **`PASS`** | `COMPLETED` | Aged priority-2 task (`effective_priority=10.0`) promoted ahead of fresh priority-6 task; `starvation=false` | [`T06.json`](artifacts/test-results/run-20261024T052934Z/T06.json) |
| **`T07`** | Policy E (`ai_assisted`) Multi-Objective Scoring & Fallback | **`PASS`** | `COMPLETED` | Normal run uses `v1.0.0-cpu-gbr` (`0` fallbacks); simulated outage triggers `fallback-heuristic-v1` on `12/12` tasks | [`T07.json`](artifacts/test-results/run-20261024T052934Z/T07.json) |
| **`T08`** | Offline 5-Policy Comparison (`n=25, 50, 100`) & Hypotheses | **`PASS`** | `COMPLETED` | `H1` (`94.07%`), `H2` (`12.77%` wait, `76%` vs `28%` deadlines), `H3` (`0` violations) all supported | [`T08.json`](artifacts/test-results/run-20261024T052934Z/T08.json) |
| **`T09`** | Kubernetes Manifests, RBAC, Non-Root & Image Digests | **`PASS`** | `COMPLETED` | Pinned `@sha256` digests verified on running Pods; non-root `65532:65532` & least-privilege RBAC verified | [`T09.json`](artifacts/test-results/run-20261024T052934Z/T09.json) |
| **`T10`** | Live GKE `batch/v1` Job Creation, Execution & Logs | **`PASS`** | `COMPLETED` | `6/6` Kubernetes Jobs `Succeeded` in `16.42s`; worker JSON logs & container durations captured | [`T10.json`](artifacts/test-results/run-20261024T052934Z/T10.json) |
| **`T11`** | Live GKE Concurrency (`c=2, 4`) & Resource Budget Gate | **`PASS`** | `COMPLETED` | `c=2`: peak active `2`, peak CPU `550m/600m`; `c=4`: peak active `4`, peak CPU `1050m/1400m` | [`T11.json`](artifacts/test-results/run-20261024T052934Z/T11.json) |
| **`T12`** | Live GKE 5-Policy Benchmark & Runtime Prediction Accuracy | **`PASS`** | `COMPLETED` | All 5 policies completed on GKE (`seed=108`); live prediction MAE `0.3206s` vs baseline `1.9667s` | [`T12.json`](artifacts/test-results/run-20261024T052934Z/T12.json) |
| **`T13`** | Idempotent Submission & Optimistic Concurrency (`ResourceVersion`) | **`PASS`** | `COMPLETED` | Duplicate `IdempotencyKey` returns `deduplicated_count=1`; stale `ResourceVersion` returns `ErrConflict` | [`T13.json`](artifacts/test-results/run-20261024T052934Z/T13.json) |
| **`T14`** | Deadline Attainment, Percentiles (`p50/p95/p99`) & Fairness | **`PASS`** | `COMPLETED` | `ai_assisted` `76.0%` deadlines vs `fifo` `28.0%`; Jain's fairness `0.5542`; `0` starved tasks | [`T14.json`](artifacts/test-results/run-20261024T052934Z/T14.json) |
| **`T15`** | Prometheus `/metrics`, `PodMonitoring` & Cloud Monitoring | **`PASS`** | `COMPLETED` | All `scheduler_*` metrics exposed; `2` `PodMonitoring` CRDs active; `1,323` Cloud Monitoring `timeSeries` points | [`T15.json`](artifacts/test-results/run-20261024T052934Z/T15.json) |
| **`T16`** | Durable `ConfigMap` State (`scheduler-durable-state`) | **`PASS`** | `COMPLETED` | `scheduler-durable-state` synced with `resourceVersion` and verified across Pod restart | [`T16.json`](artifacts/test-results/run-20261024T052934Z/T16.json) |
| **`T17`** | Terraform Zero-Drift, Workload Identity & Internal Services | **`PASS`** | `COMPLETED` | `terraform fmt/validate/plan -detailed-exitcode` exit `0` (`0` drift); all Services `ClusterIP` | [`T17.json`](artifacts/test-results/run-20261024T052934Z/T17.json) |
| **`T18`** | Deterministic Reproducibility & Historical Preservation | **`PASS`** | `COMPLETED` | Repeated `seed=42` comparisons produce identical SHA-256 fingerprint & metrics; prior runs preserved | [`T18.json`](artifacts/test-results/run-20261024T052934Z/T18.json) |
| **`T19`** | Persistent Post-Suite Cloud Environment Health Check | **`PASS`** | `COMPLETED` | Cluster `RUNNING`, `scheduler-controller` `1/1 Ready`, `runtime-predictor` `1/1 Ready`, `terraform_destroy=false` | [`T19.json`](artifacts/test-results/run-20261024T052934Z/T19.json) |

---

## 6. Diagnostic Log: Initial `T04` Failure and Fix

During the initial full run ([`artifacts/test-results/run-20261024T052347Z/T04.json`](artifacts/test-results/run-20261024T052347Z/T04.json)), `19/20` tests passed and `T04` reported `FAIL`:
- **Observed Symptom:** `ordered_predicted_durations_sec` across all 20 tasks was `[1.6365, 1.3671, 1.5117, 1.6731, 1.9461, 1.9643, 1.9842, 2.1733]`.
- **Root Cause:** In discrete-event simulation with staggered arrivals (`arrival_offset_sec`), task `#1` (`predicted_duration_sec = 1.6365s`) arrives at `t = 0.0s` when the cluster is idle and no other task has arrived yet (`queue_wait_sec = 0.0s`). A work-conserving non-preemptive scheduler immediately admits task `#1` at `t = 0.0s`. While task `#1` executes, tasks `#2..#20` arrive and wait in the queue (`queue_wait_sec > 0`), and all `19` queued tasks are admitted in strictly monotonic order (`1.3671 <= 1.5117 <= 1.6731 <= 1.9461 <= 1.9643 <= 1.9842 <= 2.1733 <= 2.2089`).
- **Resolution:** Updated `test_t04` in [`scripts/run_test_suite.py`](scripts/run_test_suite.py) to evaluate monotonicity across the queued tasks (`queue_wait_sec > 0`) while recording `initial_zero_wait_task_predicted_sec` separately. Preserved `run-20261024T052347Z` intact and re-ran `--mode full` in `run-20261024T052934Z` (`20/20 PASS`).
