# Tutorial 06: Kubernetes Jobs, Resource Limits, and Capacity Constraints

## 1. Learning Objectives

- Configure bounded `batch/v1` Kubernetes `Job` specifications with explicit CPU/memory `requests` and `limits`, `activeDeadlineSeconds`, `backoffLimit`, and `ttlSecondsAfterFinished`.
- Understand node allocatable capacity math on `e2-standard-4` (`3920m` allocatable CPU, `~13.55 GiB` allocatable RAM) vs system DaemonSet overhead.
- Verify container security contexts (`runAsNonRoot: true`, `runAsUser: 65532`, `readOnlyRootFilesystem: true`, `seccompProfile: RuntimeDefault`).

---

## 2. Bounded Job Specification Anatomy

Every Kubernetes `Job` created by [`internal/kubernetes/client.go:CreateOrGetBatchJob`](../../internal/kubernetes/client.go) enforces:

- `spec.backoffLimit: 1` and `spec.template.spec.restartPolicy: Never`: Bounded retry behavior so a failing batch task cannot loop indefinitely.
- `spec.activeDeadlineSeconds: 45`: Hard execution lease; tasks exceeding 45 seconds are terminated by Kubernetes with `reason: DeadlineExceeded` and recorded by the controller as `Status: TimedOut`.
- `spec.ttlSecondsAfterFinished: 600`: Retains completed Jobs and Pods for 10 minutes for inspection, then allows the Kubernetes TTL controller to garbage-collect finished Pods after results have been persisted in `scheduler-durable-state`.
- Explicit `resources.requests` (`150m–400m` CPU, `64Mi–384Mi` RAM) and `resources.limits`.

---

## 3. Hands-On Verification

Apply the standalone sample worker Job in [`deploy/jobs/sample-benchmark-job.yaml`](../../deploy/jobs/sample-benchmark-job.yaml):

```bash
kubectl apply -f deploy/jobs/sample-benchmark-job.yaml
kubectl -n ai-workload-scheduler wait --for=condition=complete job/sample-cpu-worker-job --timeout=60s
kubectl -n ai-workload-scheduler logs job/sample-cpu-worker-job
```
