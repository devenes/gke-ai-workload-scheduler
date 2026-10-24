# Tutorial 07: Queue and Admission Management (Custom Gate vs. Kueue)

## 1. Learning Objectives

- Understand how [`internal/dispatcher/admission.go`](../../internal/dispatcher/admission.go) enforces `MaxConcurrentJobs`, `MaxCPUBudgetMilli`, and `MaxMemoryBudgetMB`.
- Understand how durable queue state in [`internal/queue/store.go`](../../internal/queue/store.go) uses `ResourceVersion` optimistic concurrency control (`ErrConflict`) and Kubernetes `ConfigMap` (`scheduler-durable-state`) persistence.
- Understand why dynamic continuous AI scoring requires a single authoritative queue order and how Kueue (`kueue.x-k8s.io/v1beta1`) relates to custom policy ranking.

---

## 2. Avoiding Competing Scheduling Authorities

When integrating a custom AI-assisted scheduler with Kubernetes admission systems:
1. If an application ranks tasks using a **time-varying continuous score** (`schedule_score` with continuous aging and deadline slack) and then submits all Jobs at once to a Kueue `LocalQueue`, Kueue holds the Jobs suspended and admits them according to static integer `WorkloadPriorityClass`.
2. Unless the application withholds Job creation until admission capacity is available, Kueue's static priority ordering will reorder waiting tasks and override the AI scheduler's dynamic score!
3. Therefore, `gke-ai-workload-scheduler` gates admission in `dispatcher.Gate` (`TryAdmit` / `Release`) and creates the Kubernetes `batch/v1` `Job` at the exact instant capacity is approved.

---

## 3. Hands-On Verification

Run the admission gate and optimistic concurrency tests:

```bash
go test -v ./internal/scheduler -run "TestAdmissionGateConcurrencyAndResourceBudgets|TestStoreIdempotencyAndOptimisticConcurrency"
```

Inspect the live durable state ConfigMap in GKE:

```bash
kubectl -n ai-workload-scheduler get configmap scheduler-durable-state -o jsonpath='{.metadata.resourceVersion}{"\n"}'
```
