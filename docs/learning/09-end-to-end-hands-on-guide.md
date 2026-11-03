# Tutorial 09: End-to-End Hands-On Guide — Fresh Checkout to Verified Running Benchmark

## 1. Learning Objectives

Execute the complete workflow from a fresh checkout of `.` through local validation, Terraform verification, GKE deployment, live Kubernetes Job benchmarking, and the `T00`–`T19` verification suite.

---

## 2. Step-by-Step Walkthrough

### Step 1: Verify Local Toolchain and Train the CPU Predictor

```bash
cd .
export CLOUDSDK_PYTHON=/opt/homebrew/bin/python3.13

# Train the scikit-learn runtime predictor and verify holdout metrics
make train-predictor

# Run Go and Python unit/integration tests
make test
```

### Step 2: Run an Offline 5-Policy Simulation Comparison

```bash
make simulate
```

### Step 3: Validate Terraform Infrastructure in `your-gcp-project-id` (`us-west4-a`)

```bash
make tf-validate
cd infra/terraform && terraform plan -detailed-exitcode && cd ../..
```

### Step 4: Build & Push `linux/amd64` Container Images and Deploy to GKE

```bash
./scripts/deploy.sh
```

### Step 5: Open the Web UI via `kubectl port-forward`

In a dedicated terminal:

```bash
./scripts/port_forward.sh
```

Open **http://127.0.0.1:8080** in your browser to explore:
- **01 Overview**: Live GKE cluster status, queue depth, active policy, predictor holdout MAE, and recent runs.
- **02 Workload Queue**: Per-task arrival sequence, predicted duration, dispatch rank, and decision rationale.
- **03 Scheduling Lab**: Interactive controls to run offline simulations or bounded live GKE benchmarks.
- **04 Benchmark Comparison**: Side-by-side 5-policy comparison table and hypothesis (`H1`, `H2`, `H3`) verification cards.
- **05 Task Explorer**: Click any task to inspect its `ScoreBreakdown`, Kubernetes Job status, and container logs.
- **06 Observability**: Predictor holdout metadata, live `/metrics` preview, and Cloud Monitoring dashboard link.

### Step 6: Run the Full Automated Verification Suite (`T00`–`T19`)

```bash
make verify-full
make verify-repro
```

### Step 7: Safe Teardown After Preserving Evidence

Once all `T00`–`T19` verification artifacts in `artifacts/test-results/` are saved locally:

```bash
cd infra/terraform
terraform plan -destroy -out=destroy.tfplan
terraform apply "destroy.tfplan"
rm -f destroy.tfplan
```

See [`../TEARDOWN_REPORT.md`](../TEARDOWN_REPORT.md) for the verified post-destroy inventory.
