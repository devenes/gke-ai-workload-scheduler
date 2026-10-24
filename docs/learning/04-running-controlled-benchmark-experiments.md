# Tutorial 04: Running Controlled Benchmark Experiments

## 1. Learning Objectives

- Run deterministic offline simulations across all 5 scheduling policies using identical seeded batches.
- Launch bounded live GKE benchmarks that execute real Kubernetes `batch/v1` Jobs on `cpu-worker-pool`.
- Understand why task count (`TaskCount`) and concurrency limit (`Concurrency`) are independent controls.

---

## 2. Offline Simulation vs. Live GKE Execution

| Dimension | Offline Simulation (`mode: "simulation"`) | Live GKE Execution (`mode: "live_gke"`) |
|---|---|---|
| **Runtime Predictor** | Real HTTP calls to `runtime-predictor` (`/predict`) | Real HTTP calls to `runtime-predictor` (`/predict`) |
| **Policy & Admission Gate** | Real `policies.RankTasks` + `dispatcher.Gate` | Real `policies.RankTasks` + `dispatcher.Gate` |
| **Task Execution** | Discrete-event simulation using calibrated task duration | Real Kubernetes `batch/v1` `Job` Pods running `/app/scheduler-worker` on GKE |
| **Use Case** | Fast development iteration & large-scale (`n=50, 100`) sweeps | Ground-truth measurement of Kubernetes API, `kube-scheduler`, and container runtime behavior |

---

## 3. Step-by-Step Hands-On Actions

### Step 1: Run a 5-Policy Offline Comparison via CLI

```bash
go run ./cmd/scheduler -mode=simulate -local-mode=true \
  -scenario=deadline_sensitive -seed=42 -task-count=24 -concurrency=4 | jq '.hypotheses'
```

### Step 2: Run a Bounded Live GKE Benchmark via REST API

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/benchmarks/live \
  -H "Content-Type: application/json" \
  -d '{
    "policy": "ai_assisted",
    "scenario": "deadline_sensitive",
    "seed": 108,
    "task_count": 6,
    "concurrency": 3,
    "async": false
  }' | jq '{run_id, mode, policy, batch_fingerprint_sha256, metrics}'
```

### Step 3: Inspect Persisted Benchmark Artifacts

Every benchmark run is automatically persisted in `artifacts/benchmarks/<run_id>.json` and mirrored to the Kubernetes ConfigMap `scheduler-durable-state`.
