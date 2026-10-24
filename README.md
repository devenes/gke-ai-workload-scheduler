# GKE AI Workload Scheduler (`gke-ai-workload-scheduler`)

A complete, reproducible, CPU-only **AI-assisted batch workload scheduling platform** implemented, deployed, benchmarked, and verified end-to-end on **Google Kubernetes Engine (GKE) Standard** (`gke-ai-workload-scheduler`, `us-west4-a`, project `enes-turan-demo`).

This platform demonstrates how a CPU-only `scikit-learn` runtime predictor (`GradientBoostingRegressor`, `v1.0.0-cpu-gbr`) predicts container execution duration prior to admission (`0.2443s` holdout MAE vs. `2.2836s` historical class median baseline — **`89.30%` error reduction**, and **`0.3206s` live GKE MAE**) and how a multi-objective scheduling policy (**Policy E: `ai_assisted`**) uses those predictions alongside deadline slack urgency, resource pressure, priority, and bounded aging to improve queue waiting time and deadline attainment on both offline discrete-event simulations (`n=25, 50, 100`) and real Kubernetes `batch/v1` `Job` workloads on GKE (`n=5, 6, 8, 10`).

---

## Final Cloud Lifecycle Status & Cost Summary

> **POST-TEARDOWN STATUS (`DESTROYED — $0.00 / hr`):**  
> After completing and preserving all live GKE benchmarks, `T00`–`T19` full verification (`20/20 PASS`), and reproducibility verification (`7/7 PASS`), the 24 Terraform-managed GCP resources created specifically for this demo were destroyed via a reviewed `terraform plan -destroy -out=destroy.tfplan` and independently verified absent (see [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md)).  
> To recreate the live GKE environment from a fresh checkout, follow [Section 5 (Quickstart)](#5-quickstart-local-simulation-gke-redeployment--verification) below.

| Time Horizon (When Provisioned) | Compute (`1 x e2-standard-4`) | Boot Disk (`50 GB pd-balanced`) | GKE Zonal Management (`Free Tier`) | Storage, Registry & Monitoring | **Estimated Active Cost (USD)** |
|---|---:|---:|---:|---:|---:|
| **Current Post-Destroy State** | `$0.0000` | `$0.0000` | `$0.0000` | `$0.0000` | **`$0.0000 / hr` (Destroyed)** |
| **1 Hour (Active Stack)** | `$0.1509` | `$0.0075` | `$0.0000` (Covered by `$74.40/mo` credit) | `$0.0005` | **`$0.1590 / hr`** |
| **1 Day (24 Hours Active)** | `$3.6227` | `$0.1808` | `$0.0000` | `$0.0120` | **`$3.82 / day`** |
| **1 Month (730 Hours Active)** | `$110.19` | `$5.50` | `$0.00` (`$73.00` gross `- $73.00` credit) | `$0.36` | **`$116.05 / month`** |
| **Single Benchmark Run (`~15 min`)** | `$0.0377` | `$0.0019` | `$0.0000` | `$0.0001` | **`$0.040 / run`** |

See [`COST_AND_QUOTAS.md`](COST_AND_QUOTAS.md) and [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md) for full quota, pricing, and post-destroy verification tables.

---

## 1. Verified Cloud Configuration & Image Digests

| Resource / Component | Identifier / Value | Verification Status |
|---|---|---|
| **GCP Project** | `enes-turan-demo` (`697289276442`) | Verified (`us-west4` / `us-west4-a`) |
| **GKE Cluster** | `gke-ai-workload-scheduler` (`us-west4-a`, GKE `1.36.4-gke.1495002`, Standard Zonal) | Verified Live (`T00`–`T19`), then Destroyed |
| **Node Pool** | `cpu-worker-pool` (`1 x e2-standard-4`, `4 vCPU`, `16 GiB RAM`, `50 GB pd-balanced`, `3920m` allocatable CPU) | Verified Live (`1 Ready`), then Destroyed |
| **Kubernetes Namespace** | `ai-workload-scheduler` | Verified (`ClusterIP` only, zero external LB) |
| **Controller Deployment** | `deployment/scheduler-controller` (`ClusterIP` `8080/TCP`, non-root `65532:65532`) | Verified `1/1 Ready` |
| **Predictor Deployment** | `deployment/runtime-predictor` (`ClusterIP` `8081/TCP`, non-root `65532:65532`) | Verified `1/1 Ready` |
| **Durable State Backing Store** | `ConfigMap/scheduler-durable-state` + per-run `ConfigMap/run-*` records | Verified across Pod restart (`T16`) |
| **Scheduler & Worker Image** | `us-west4-docker.pkg.dev/enes-turan-demo/ai-workload-scheduler/scheduler@sha256:30ce4173ac008506022e0cd95b12cf2636ad69cdb290715d5169678243642e08` | Recorded in [`artifacts/deployed_images.json`](artifacts/deployed_images.json) |
| **Predictor Image** | `us-west4-docker.pkg.dev/enes-turan-demo/ai-workload-scheduler/predictor@sha256:353705e220532916a4430c62d50fce0bf52ceb0380c3ca879243db300b4f0c39` | Recorded in [`artifacts/deployed_images.json`](artifacts/deployed_images.json) |
| **Cloud Monitoring Dashboard** | `projects/697289276442/dashboards/23d1f9c0-438e-4b95-a7cc-6c8299d5a5d4` | Verified (`1,323` `timeSeries` points in `T15`) |

---

## 2. System Architecture

```
+---------------------------------------------------------------------------------------------------+
|                           Browser Web UI (6 Tabs + Live SSE Stream)                               |
|                Access locally via: ./scripts/port_forward.sh -> http://127.0.0.1:8080             |
+-------------------------------------------------+-------------------------------------------------+
                                                  | HTTP REST + SSE (/api/v1/*, /metrics)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                     GKE Cluster: gke-ai-workload-scheduler (namespace: ai-workload-scheduler)     |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  | Deployment: scheduler-controller (Go 1.27, non-root 65532:65532)                            |  |
|  |                                                                                             |  |
|  |  1. Workload Generator & Validator (internal/workload)                                      |  |
|  |     - 7 archetypes: short_cpu, long_cpu, variable_size, memory_sensitive,                   |  |
|  |       deadline_sensitive, mixed, priority                                                   |  |
|  |     - Deterministic SHA-256 batch fingerprinting                                            |  |
|  |                                                                                             |  |
|  |  2. 5 Pluggable Ranking Policies (internal/policies)                                        |  |
|  |     - Policy A: fifo            (Arrival order)                                             |  |
|  |     - Policy B: sjf             (Shortest predicted runtime first)                          |  |
|  |     - Policy C: edf             (3-tier Earliest Deadline First + slack filter)             |  |
|  |     - Policy D: priority_aging  (Static priority + bounded wait aging)                      |  |
|  |     - Policy E: ai_assisted     (Multi-objective ML score + explainable breakdown)          |  |
|  |                                                                                             |  |
|  |  3. Admission Gate (internal/dispatcher)       4. Durable Queue Store (internal/queue)      |  |
|  |     - MaxConcurrentJobs cap (1..16)               - Optimistic locking (ResourceVersion)    |  |
|  |     - CPU & Memory budget caps                    - IdempotencyKey deduplication            |  |
|  |     - Separates policy rank, admission,           - Mirrors state to ConfigMap              |  |
|  |       and kube-scheduler Pod start                  scheduler-durable-state                 |  |
|  +------------------------------------+-----------------------------------+--------------------+  |
|                                       | HTTP POST /predict                | client-go batch/v1    |
|                                       v                                   v                       |
|  +-----------------------------------------+     +---------------------------------------------+  |
|  | Deployment: runtime-predictor           |     | Real GKE Worker Jobs (batch/v1 Job)         |  |
|  | (Python 3.12 + scikit-learn 1.5.2)      |     | (/app/scheduler-worker)                     |  |
|  | - Model: GradientBoostingRegressor      |     | - Bounded SHA-256 CPU loops + memory touch  |  |
|  | - 17 pre-execution features             |     | - Emits structured JSON timing & digest     |  |
|  | - Rejects actual_duration_sec leakage   |     | - Non-root 65532:65532, read-only rootfs    |  |
|  | - Automatic heuristic fallback          |     +---------------------------------------------+  |
|  +-----------------------------------------+                                                      |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Five Scheduling Policies & Multi-Objective AI Formula

All 5 policies are implemented in [`internal/policies/policies.go`](internal/policies/policies.go) and attach a structured `ScoreBreakdown` and human-readable `decision_reason` to every task:

1. **Policy A — `fifo` (First-In, First-Out):** Ranks waiting tasks strictly by `arrival_sequence` and `submitted_at`.
2. **Policy B — `sjf` (Shortest Predicted Job First):** Ranks waiting tasks by pre-execution `predicted_duration_sec` (when the ML predictor is active) or `historical_class_mean_sec` (without oracle access to actual duration).
3. **Policy C — `edf` (Earliest Deadline First with Slack Filter):** Separates tasks into **Tier 1** (tasks with deadlines and positive slack `remaining_sec - est_duration_sec >= -0.5s`, ordered by earliest deadline), **Tier 2** (tasks without deadlines, ordered by priority/arrival), and **Tier 3** (expired deadlines, ordered by oldest wait so they cannot starve).
4. **Policy D — `priority_aging` (Priority with Bounded Aging):** Computes `effective_priority = min(10 + MaxAgingMultiple, priority + floor(wait_sec / AgingWindowSec) * AgingBoostPerStep)` and ranks highest effective priority first.
5. **Policy E — `ai_assisted` (Multi-Objective AI-Assisted Policy):** Minimizes the composite cost function (`lower score = higher dispatch priority`):

$$\text{Score}(t) = W_{\text{runtime}} \cdot \hat{C}_{\text{run}}(t) - W_{\text{deadline}} \cdot U_{\text{dl}}(t) + W_{\text{resource}} \cdot P_{\text{res}}(t) - W_{\text{priority}} \cdot B_{\text{prio}}(t) - W_{\text{aging}} \cdot B_{\text{age}}(t)$$

where default weights (`DefaultWeights()`) are $W_{\text{runtime}} = 1.0$, $W_{\text{deadline}} = 2.6$, $W_{\text{resource}} = 0.35$, $W_{\text{priority}} = 0.85$, $W_{\text{aging}} = 1.4$ (with anti-starvation cap `MaxAgingMultiple = 2.5`).

---

## 4. Key Measured Results Summary

Full experimental details, hypotheses (`H1`–`H3`), and per-test evidence links are documented in [`EXPERIMENTS.md`](EXPERIMENTS.md).

### 4.1 CPU Runtime Predictor Holdout Performance (`seed=202602`, `n=350`)
- **Holdout MAE:** **`0.2443s`** vs. Per-Class Median Baseline **`2.2836s`** (**`89.30%` error reduction**) and Global Median Baseline **`3.6812s`** (**`93.36%` error reduction**).
- **Holdout RMSE:** **`0.3750s`** vs. Per-Class Median Baseline **`3.8290s`** (**`90.21%` reduction**); **$R^2 = 0.9946$**.

### 4.2 Offline 5-Policy Simulation Comparison (`seed=42`, `concurrency=4`)

| Scenario & Size | Policy E (`ai_assisted`) | Policy C (`edf`) | Policy B (`sjf`) | Policy D (`priority_aging`) | Policy A (`fifo`) |
|---|---|---|---|---|---|
| **`deadline_sensitive` (`n=25`)**<br>Mean Wait / Deadlines | **`10.96s` / `76.00%` (`19/25`)** | `11.14s` / `72.00%` (`18/25`) | `9.96s` / `52.00%` (`13/25`) | `11.74s` / `48.00%` (`12/25`) | `12.57s` / `28.00%` (`7/25`) |
| **`mixed` (`n=50`)**<br>Mean Wait / Deadlines | **`15.00s` / `100.00%` (`30/30`)** | `16.80s` / `100.00%` (`30/30`) | `12.19s` / `76.67%` (`23/30`) | `17.62s` / `66.67%` (`20/30`) | `22.29s` / `33.33%` (`10/30`) |
| **`mixed` (`n=100`)**<br>Mean Wait / Deadlines | **`32.70s` / `98.15%` (`53/54`)** | `39.22s` / `100.00%` (`54/54`) | `25.46s` / `68.52%` (`37/54`) | `39.69s` / `33.33%` (`18/54`) | `47.58s` / `18.52%` (`10/54`) |

### 4.3 Live GKE `batch/v1` Job 5-Policy Comparison (`T12`, `deadline_sensitive`, `seed=108`, `n=6`, `concurrency=3`)

| Policy (Live GKE Jobs) | Live Makespan (s) | Mean Queue Wait (s) | Deadline Attainment | Live Prediction MAE (s) |
|---|---:|---:|---:|---:|
| **Policy E: `ai_assisted`** | `13.54s` | **`3.80s`** (`-7.40%` vs FIFO, `-15.07%` vs EDF) | **`83.33%` (`5/6`)** | **`0.3206s`** (vs `1.9667s` baseline) |
| **Policy B: `sjf`** | `13.72s` | `3.49s` | `83.33%` (`5/6`) | `0.3788s` |
| **Policy D: `priority_aging`** | `15.33s` | `4.37s` | `83.33%` (`5/6`) | `0.3206s` |
| **Policy C: `edf`** | `16.29s` | `4.48s` | `83.33%` (`5/6`) | `0.5359s` |
| **Policy A: `fifo`** | `12.10s` | `4.11s` | `66.67%` (`4/6`) | `0.4455s` |

---

## 5. Quickstart: Local Simulation, GKE Redeployment & Verification

### 5.1 Run Offline Simulation & Unit Tests Locally (Zero Cloud Cost)
```bash
# 1. Run Go and Python unit test suites
make test

# 2. Train CPU runtime predictor and evaluate holdout accuracy
make train-predictor

# 3. Run deterministic 5-policy offline simulation comparison
make simulate

# 4. Start local controller + predictor + Web UI on http://127.0.0.1:8080
.venv/bin/python -m predictor.app &
LOCAL_MODE=true go run ./cmd/scheduler
```

### 5.2 Provision GKE Cluster, Deploy Images & Access Web UI
```bash
# 1. Validate, plan, and apply Terraform infrastructure in enes-turan-demo (us-west4-a)
make tf-validate
make tf-plan
make tf-apply

# 2. Build linux/amd64 images, push to Artifact Registry, and deploy Kustomize overlay to GKE
make deploy

# 3. Port-forward the Web UI and REST API to http://127.0.0.1:8080
./scripts/port_forward.sh
```

### 5.3 Run the Automated Verification Suite (`T00`–`T19`) and Safe Teardown
```bash
# Run full 20-test live GKE verification suite (T00..T19)
make verify-full

# Run safe non-destructive reproducibility subset (T00, T01, T02, T08, T17, T18, T19)
make verify-repro

# When finished, destroy all project-owned GCP resources safely
./scripts/future_teardown.sh --confirm-destroy
```
- **Primary Full Live GKE Verification Evidence (`20/20 PASS`):** [`artifacts/test-results/run-20261024T052934Z/summary.json`](artifacts/test-results/run-20261024T052934Z/summary.json)
- **Reproducibility Verification Evidence (`7/7 PASS`):** [`artifacts/test-results/repro-20261024T053407Z/summary.json`](artifacts/test-results/repro-20261024T053407Z/summary.json)
- **Progressive Live GKE Scaling Evidence (`n=5, 10`):** [`artifacts/benchmarks/progressive_live_gke_scaling.json`](artifacts/benchmarks/progressive_live_gke_scaling.json)

---

## 6. Documentation & Learning Curriculum

### Root Engineering & Operational Documents
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — Control plane, admission gate, 5 policies, durable state, and security model.
- [`EXPERIMENTS.md`](EXPERIMENTS.md) — Predeclared hypotheses (`H1`–`H3`), holdout metrics, offline simulations (`n=25, 50, 100`), live GKE benchmarks, and `T00`–`T19` evidence index.
- [`OPERATIONS.md`](OPERATIONS.md) — Runbook for GKE provisioning, deployment, port-forwarding, monitoring verification, troubleshooting, and safe teardown.
- [`FEASIBILITY.md`](FEASIBILITY.md) — Node sizing, allocatable CPU/memory budget math, and risk register.
- [`COST_AND_QUOTAS.md`](COST_AND_QUOTAS.md) — Live GCP quota audit, active runtime cost tables, and post-teardown `$0.00/hr` billing status.
- [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md) — Evidence-backed Terraform destroy audit and independent post-destroy GCP inventory verification.

### 9-Part Hands-On Tutorial Series (`docs/learning/`)
1. [`01-kubernetes-batch-scheduling-fundamentals.md`](docs/learning/01-kubernetes-batch-scheduling-fundamentals.md) — Queue Ranking vs. Admission Gating vs. `kube-scheduler` Placement
2. [`02-scheduling-policies-fifo-sjf-edf-priority-aging.md`](docs/learning/02-scheduling-policies-fifo-sjf-edf-priority-aging.md) — Deep Dive into `FIFO`, `SJF`, `EDF`, `Priority+Aging`, and `AI-Assisted` Scoring
3. [`03-runtime-prediction-cpu-scikit-learn.md`](docs/learning/03-runtime-prediction-cpu-scikit-learn.md) — Feature Engineering, Anti-Leakage Contracts & CPU `GradientBoostingRegressor`
4. [`04-running-controlled-benchmark-experiments.md`](docs/learning/04-running-controlled-benchmark-experiments.md) — Running Deterministic Offline Simulations and Live GKE Benchmarks
5. [`05-interpreting-latency-throughput-fairness-deadlines.md`](docs/learning/05-interpreting-latency-throughput-fairness-deadlines.md) — Interpreting `p50/p95/p99` Latency, Jain's Fairness Index, and Deadline Attainment
6. [`06-kubernetes-jobs-resource-limits-capacity.md`](docs/learning/06-kubernetes-jobs-resource-limits-capacity.md) — Kubernetes `batch/v1` Jobs, Resource Requests/Limits & Node Allocatable Math
7. [`07-queue-and-admission-management.md`](docs/learning/07-queue-and-admission-management.md) — Durable `ConfigMap` State, Idempotency & Optimistic Concurrency (`ResourceVersion`)
8. [`08-monitoring-and-troubleshooting.md`](docs/learning/08-monitoring-and-troubleshooting.md) — Prometheus Metrics, `PodMonitoring`, Cloud Monitoring & Troubleshooting
9. [`09-end-to-end-hands-on-guide.md`](docs/learning/09-end-to-end-hands-on-guide.md) — Complete Hands-On Guide from Fresh Checkout to Verified Benchmark and Teardown
