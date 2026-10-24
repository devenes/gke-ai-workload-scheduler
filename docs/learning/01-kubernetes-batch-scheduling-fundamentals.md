# Tutorial 01: Kubernetes Batch Scheduling Fundamentals

## 1. Learning Objectives

- Distinguish between **application-level queue ranking**, **capacity admission gating**, and **node-level Kubernetes Pod placement (`kube-scheduler`)**.
- Understand how Kubernetes `batch/v1` `Job` controllers create Pods and reconcile completion and failure states.
- Trace a task through the 9 lifecycle states (`Pending`, `Ranked`, `Submitted`, `Admitted`, `Running`, `Succeeded`, `Failed`, `TimedOut`, `Cancelled`) implemented in [`internal/workload/generator.go`](../../internal/workload/generator.go).

---

## 2. Prerequisites

- Go `1.27+`, Python `3.13+` (`uv` virtual environment at `.venv`), and `kubectl` configured for `gke-ai-workload-scheduler`.

---

## 3. Conceptual Explanation: Why Three Scheduling Stages Exist

When a batch platform receives 50 logical tasks on a 4-vCPU GKE node, creating 50 Kubernetes `Job` objects simultaneously floods the API server and leaves 46 Pods stuck in `Pending` (`Insufficient cpu`). Once a Pod is created, `kube-scheduler` orders pending Pods by static integer `PriorityClass` and creation time—it cannot dynamically re-order waiting work based on ML runtime predictions or continuous deadline slack.

Therefore, `gke-ai-workload-scheduler` separates batch scheduling into three distinct stages:

1. **Application Policy Ranking ([`internal/policies/policies.go`](../../internal/policies/policies.go))**: Ranks waiting logical tasks before Kubernetes `Job` creation.
2. **Concurrency & Resource-Budget Admission Gate ([`internal/dispatcher/admission.go`](../../internal/dispatcher/admission.go))**: Releases ranked tasks only when `MaxConcurrentJobs`, `MaxCPUBudgetMilli`, and `MaxMemoryBudgetMB` have headroom.
3. **Kubernetes Pod Scheduling ([`internal/kubernetes/client.go`](../../internal/kubernetes/client.go))**: Dispatches a `batch/v1` `Job` whose Pod is placed onto the GKE node by `kube-scheduler`.

---

## 4. Step-by-Step Hands-On Verification

### Step 1: Inspect the Task Lifecycle State Machine in Code

Run the Go unit test suite exercising queue submission and state transitions:

```bash
go test -v ./internal/scheduler -run TestStoreIdempotencyAndOptimisticConcurrency
```

### Step 2: Submit a Batch to the Queue and Inspect Ranked State

With `./scripts/port_forward.sh` running:

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{"scenario":"mixed","seed":42,"task_count":5,"policy":"ai_assisted"}' | jq '.tasks[] | {task_id, arrival_sequence, dispatch_rank, status, decision_reason}'
```

### Step 3: Verify Lifecycle Timestamps on a Live GKE Benchmark

```bash
curl -s "http://127.0.0.1:8080/api/v1/tasks?limit=5" | jq '.tasks[] | {task_id, status, submitted_at, admitted_at, job_created_at, pod_scheduled_at, started_at, finished_at}'
```

---

## 5. Limitations & Common Failure Cases

- **Conflating Dispatch Rank with Pod Start Sequence**: Even when Task A is admitted 10ms before Task B (`AdmissionSequence: 1` vs `2`), Kubernetes container image pull or kubelet startup jitter can occasionally start Container B a few milliseconds before Container A. Always record `DispatchRank`, `AdmissionSequence`, and `PodStartSequence` separately.
