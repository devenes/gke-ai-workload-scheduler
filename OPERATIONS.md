# Operations and Troubleshooting Runbook — `gke-ai-workload-scheduler`

> [!NOTE]
> **Cloud Lifecycle Status (`DESTROYED — $0.00 / hr`)**: Following completion of all live GKE benchmarks and `T00`–`T19` verification (`20/20 PASS`), the 24 Terraform-managed GCP resources were destroyed via a reviewed `terraform plan -destroy -out=destroy.tfplan` and verified absent (see [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md)). Follow Sections 3 and 4 below to recreate the infrastructure at any time.

---

## 1. Inspecting the Project and Live GKE Cluster

```bash
export CLOUDSDK_PYTHON=/opt/homebrew/bin/python3.13
export PROJECT_ID=enes-turan-demo
export REGION=us-west4
export ZONE=us-west4-a
export CLUSTER_NAME=gke-ai-workload-scheduler
export K8S_NAMESPACE=ai-workload-scheduler

# Verify GCP identity and cluster status
gcloud auth list
gcloud container clusters list --project="${PROJECT_ID}"
gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"

# Inspect nodes, deployments, pods, services, and PodMonitoring
kubectl get nodes -o wide
kubectl -n "${K8S_NAMESPACE}" get deployments,pods,svc,podmonitoring,configmaps
```

---

## 2. Local Testing and Predictor Training (Zero Cloud Cost)

```bash
# 1. Train the CPU-only scikit-learn predictor and verify holdout metrics
make train-predictor

# 2. Run Go and Python unit/integration test suites
make test

# 3. Run an offline 5-policy simulation comparison
make simulate
```

---

## 3. Terraform Validation, Planning, and Provisioning

```bash
cd infra/terraform

# Validate formatting and configuration
terraform fmt -check
terraform validate

# Inspect current state and verify zero drift
terraform state list
terraform plan -detailed-exitcode
```

---

## 4. Building Container Images and Deploying to GKE

```bash
# Build linux/amd64 images, push to Artifact Registry, pin digests, and deploy via Kustomize
./scripts/deploy.sh
```

Verify the deployed immutable image digests on the running Pods:

```bash
kubectl -n ai-workload-scheduler get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

---

## 5. Opening the Web UI and API via Port-Forwarding

Because all Kubernetes Services use internal `ClusterIP` for security, access the Web UI and REST API via `kubectl port-forward`:

```bash
./scripts/port_forward.sh
```

Then open **http://127.0.0.1:8080** in your browser.

> **Reconnecting After Terminal Closure**:
> `kubectl port-forward` is a foreground tunnel bound to your local terminal session. It does not survive terminal closure or Pod restarts. To reconnect at any time, simply run `./scripts/port_forward.sh` again.

---

## 6. Submitting Workloads and Running Benchmarks via CLI / API

While `./scripts/port_forward.sh` is running on `127.0.0.1:8080`:

```bash
# Check controller & predictor health
curl -s http://127.0.0.1:8080/healthz | jq .

# Inspect ML runtime predictor metadata and holdout MAE
curl -s http://127.0.0.1:8080/api/v1/predictor/metadata | jq '{model_version, holdout_metrics, baseline_comparison}'

# Run a 5-policy offline simulation comparison (25 tasks, concurrency 4, seed 42)
curl -s "http://127.0.0.1:8080/api/v1/benchmarks/compare?scenario=deadline_sensitive&seed=42&task_count=25&concurrency=4" | jq .

# Run a bounded live GKE benchmark creating real Kubernetes batch/v1 Jobs
curl -s -X POST http://127.0.0.1:8080/api/v1/benchmarks/live \
  -H "Content-Type: application/json" \
  -d '{"policy":"ai_assisted","scenario":"deadline_sensitive","seed":42,"task_count":8,"concurrency":4,"async":false}' | jq '{run_id, status, metrics}'
```

---

## 7. Inspecting Kubernetes Jobs, Logs, Queue State, and Metrics

```bash
# List recent Kubernetes batch/v1 Jobs and their Pods
kubectl -n ai-workload-scheduler get jobs -l app.kubernetes.io/component=benchmark-worker
kubectl -n ai-workload-scheduler get pods -l app.kubernetes.io/component=benchmark-worker -o wide

# Read worker execution logs from the latest Job Pod
kubectl -n ai-workload-scheduler logs -l app.kubernetes.io/component=benchmark-worker --tail=20

# Inspect durable queue state ConfigMap
kubectl -n ai-workload-scheduler get configmap scheduler-durable-state -o yaml | head -n 30

# View Prometheus metrics exposed by scheduler-controller
curl -s http://127.0.0.1:8080/metrics | grep "^scheduler_"
```

---

## 8. Running the Automated Verification Suite (`T00`–`T19`)

```bash
# Run the full 20-test suite (unit, simulation, live GKE Jobs, Cloud Monitoring, Terraform zero-drift, persistence)
make verify-full

# Run the safe non-destructive reproducibility subset (T00, T01, T02, T08, T17, T18, T19)
make verify-repro
```

---

## 9. Troubleshooting Guide

| Symptom | Root Cause | Resolution |
|---|---|---|
| `gcloud storage` fails with `AttributeError: module 'ssl' has no attribute 'PROTOCOL_TLSv1'` | macOS `gcloud` bundled Python 3.14 incompatibility | Export `CLOUDSDK_PYTHON=/opt/homebrew/bin/python3.13` before running `gcloud storage`. |
| `curl http://127.0.0.1:8080/healthz` returns `Connection refused` | Local `kubectl port-forward` session closed | Run `./scripts/port_forward.sh` in a terminal window. |
| Live benchmark returns `HTTP 409 Conflict` | Another live GKE benchmark is currently in progress | Wait for the active run to finish or check `GET /api/v1/benchmarks`. |
| Predictor status shows `fallback_active` | Simulated outage was toggled during `T07` testing or `runtime-predictor` Pod is restarting | Reset via `curl -X POST http://127.0.0.1:8080/api/v1/predictor/simulate-outage -d '{"force_fail":false}'` and check `kubectl -n ai-workload-scheduler get pods`. |

---

## 10. Safe Teardown Procedure

After preserving all benchmark artifacts and verification evidence locally:

```bash
cd infra/terraform
terraform plan -destroy -out=destroy.tfplan
terraform apply "destroy.tfplan"
rm -f destroy.tfplan
```

Or run the guarded helper script:

```bash
./scripts/future_teardown.sh --confirm-authorized-destroy
```

See [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md) for the complete post-destroy audit of all 24 Terraform-managed resources.
