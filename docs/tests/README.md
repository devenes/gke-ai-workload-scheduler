# Verification Suite (`T00`–`T19`) Reference

The automated verification suite is implemented in [`../../scripts/run_test_suite.py`](../../scripts/run_test_suite.py) and writes immutable timestamped evidence directories under `artifacts/test-results/run-<UTC>/` and `artifacts/test-results/repro-<UTC>/`.

| Test ID | Name | Scope |
|---|---|---|
| `T00` | Infrastructure and CPU-only preflight | GKE cluster, node pool, CPU-only check, controller connectivity |
| `T01` | Workload generator determinism | SHA-256 fingerprint determinism across all 7 workload classes |
| `T02` | Runtime predictor | Training/holdout split, feature validation, baseline comparison (`>50%` MAE reduction) |
| `T03` | FIFO ordering | Strict arrival-sequence ordering under equal conditions |
| `T04` | SJF behavior | Monotonic ordering by pre-execution predicted duration without future leakage |
| `T05` | Deadline policy | 3-tier EDF behavior for viable, expired, and missing deadlines |
| `T06` | Priority and aging | Continuous effective priority aging and zero starvation |
| `T07` | AI-assisted policy | Multi-objective scoring with ML predictor and explicit fallback on outage |
| `T08` | Offline policy comparison | 5-policy comparison at `n=25, 50, 100` and hypothesis evaluation (`H1`, `H2`, `H3`) |
| `T09` | API and UI | Input validation (`HTTP 400`), policy selection, queue inspection, and UI serving |
| `T10` | Live Kubernetes Job execution | Real `batch/v1` Jobs on GKE, completion verification, and Pod log capture |
| `T11` | Concurrency and resource budget | Live GKE concurrency (`c=2`, `c=4`) and CPU/memory budget enforcement |
| `T12` | Predicted versus actual runtime | 5-policy live GKE comparison (`seed=108`) and ML vs baseline MAE on real GKE Jobs |
| `T13` | Idempotency and failure handling | `IdempotencyKey` deduplication and `ResourceVersion` optimistic concurrency (`ErrConflict`) |
| `T14` | Deadline and fairness evaluation | Deadline attainment rates, `p50/p95/p99` waits, Jain's fairness index, starvation check |
| `T15` | Metrics and Cloud Monitoring | `/metrics` exposition, `PodMonitoring` CRDs, and live Cloud Monitoring `timeSeries` verification |
| `T16` | Queue/admission integration | Durable Kubernetes `ConfigMap` (`scheduler-durable-state`) with `resourceVersion` locking |
| `T17` | Terraform and security consistency | `terraform fmt/validate/plan -detailed-exitcode` zero drift, `ClusterIP`, Workload Identity |
| `T18` | Reproducibility | Clean reproducibility rerun (`--mode repro`) verifying identical fingerprints and metrics |
| `T19` | Persistent post-test health | Confirms cluster, node pool, deployments, and services remain running after tests |
