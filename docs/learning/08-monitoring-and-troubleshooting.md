# Tutorial 08: Monitoring and Troubleshooting

## 1. Learning Objectives

- Inspect the low-cardinality Prometheus metrics emitted by [`internal/observability/metrics.go`](../../internal/observability/metrics.go) and [`predictor/app.py`](../../predictor/app.py).
- Verify GKE Managed Service for Prometheus (`PodMonitoring`) scraping in [`deploy/base/podmonitoring.yaml`](../../deploy/base/podmonitoring.yaml).
- Query live time series from the Cloud Monitoring API and inspect the Terraform-managed dashboard ([`deploy/monitoring/dashboard.json`](../../deploy/monitoring/dashboard.json)).

---

## 2. Cardinality Discipline in Scheduler Metrics

High-cardinality identifiers such as `task_id` or `run_id` are **never** placed in Prometheus metric labels. Instead:
- Prometheus metrics use bounded low-cardinality labels (`policy`, `workload_class`, `status`, `mode`).
- Per-task execution details (`task_id`, `k8s_job_name`, `predicted_duration_sec`, `actual_duration_sec`, `score_breakdown`) are persisted in structured JSON benchmark artifacts (`artifacts/benchmarks/<run_id>.json`).

---

## 3. Step-by-Step Hands-On Verification

### Step 1: Scrape `/metrics` Locally via Port-Forward

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E "^scheduler_(tasks_completed_total|queue_wait_seconds_count|prediction_abs_error_seconds_count)"
```

### Step 2: Check `PodMonitoring` Status in GKE

```bash
kubectl -n ai-workload-scheduler get podmonitoring -o yaml
```

### Step 3: Verify Cloud Monitoring Time Series via REST API

```bash
TOKEN=$(gcloud auth print-access-token)
curl -s -H "Authorization: Bearer ${TOKEN}" \
  "https://monitoring.googleapis.com/v3/projects/enes-turan-demo/timeSeries?filter=metric.type%3D%22prometheus.googleapis.com%2Fscheduler_tasks_completed_total%2Fcounter%22&interval.startTime=$(date -u -v-30M +%Y-%m-%dT%H:%M:%SZ)&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | jq '.timeSeries | length'
```
