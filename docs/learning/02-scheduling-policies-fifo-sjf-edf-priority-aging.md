# Tutorial 02: Scheduling Policies — FIFO, SJF, EDF, Priority with Aging, and AI-Assisted

## 1. Learning Objectives

- Understand the exact mathematical ranking rules for all 5 policies implemented in [`internal/policies/policies.go`](../../internal/policies/policies.go).
- Verify how **Priority with Aging** prevents indefinite starvation of low-priority tasks.
- Verify how **EDF Tiering** prevents already-expired tasks from causing domino deadline misses of viable tasks.
- Understand how the **AI-Assisted Scheduler** combines ML runtime predictions with deadline slack, resource pressure, priority, and aging.

---

## 2. Policy Equations & Behavior

### Policy A — `fifo` (First In, First Out)
Orders tasks by `ArrivalSequence` ascending (tie-breaker: `TaskID`). Ignores job duration, priority, and deadlines.

### Policy B — `sjf` (Prediction-Based Shortest Job First)
Orders tasks by pre-execution `EstimatedDurationSec` ascending (populated by the ML runtime predictor or fallback heuristic). Minimizes mean queue waiting time (`9.60s` on `seed=42, n=24`), but can delay long tasks and ignore deadlines.

### Policy C — `edf` (Earliest Deadline First)
Partitions waiting tasks at scheduling time $t_{\text{now}}$ into three tiers:
1. **Tier 1 (Viable Deadlines, $\text{Deadline}_i \ge t_{\text{now}}$)**: Earliest deadline first.
2. **Tier 2 (Expired Deadlines, $\text{Deadline}_i < t_{\text{now}}$)**: Ordered by priority descending so expired tasks do not block viable Tier-1 tasks.
3. **Tier 3 (No Deadline)**: Ordered by priority descending, then arrival sequence.

### Policy D — `priority_aging` (Priority with Aging)
$$\text{EffectivePriority}(i, t_{\text{now}}) = \text{Priority}_i + 1.5 \cdot \frac{\max(0, t_{\text{now}} - t_{\text{submitted}, i})}{5.0\text{ s}}$$
Every 5 seconds of queue wait adds `+1.5` effective priority points, guaranteeing that a `Priority=1` task waiting `30s` reaches `EffectivePriority=10.0` and overtakes newly arrived `Priority=9` tasks.

### Policy E — `ai_assisted` (Multi-Objective AI-Assisted Scheduler)
$$\text{schedule\_score}_i = \text{predicted\_runtime\_cost}_i + \text{deadline\_risk}_i + \text{resource\_pressure}_i - \text{priority\_benefit}_i - \text{aging\_benefit}_i$$
Tasks are dispatched in ascending `schedule_score` order. Unlike blind `edf` (which only sees $\text{Deadline}_i - t_{\text{now}}$), `ai_assisted` computes **predicted slack** $s_i = (\text{Deadline}_i - t_{\text{now}}) - \hat{d}_i$, distinguishing tight achievable deadlines from infeasible ones.

---

## 3. Hands-On Verification

Run the policy unit test verifying all 5 policies on controlled tasks:

```bash
go test -v ./internal/scheduler -run TestSchedulingPoliciesBehavior
```

Inspect the policy catalog and active weights via the API:

```bash
curl -s http://127.0.0.1:8080/api/v1/policies | jq .
```
