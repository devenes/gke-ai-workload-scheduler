# Tutorial 05: Interpreting Latency, Throughput, Fairness, and Deadline Results

## 1. Learning Objectives

- Interpret the 20+ benchmark metrics computed in [`internal/scheduler/engine.go:CalculateRunMetrics`](../../internal/scheduler/engine.go).
- Understand why sample size ($n$) constrains the statistical interpretation of `p95` and `p99` tail percentiles.
- Analyze the fundamental trade-offs between **mean queue wait (SJF)**, **deadline success rate (EDF / AI-Assisted)**, and **anti-starvation fairness (Priority+Aging / AI-Assisted)**.

---

## 2. Definitions of Key Metrics

1. **Queue Wait Duration (`queue_wait_sec`)**: $t_{\text{admitted}} - t_{\text{submitted}}$. Measures time spent waiting for policy selection and admission capacity.
2. **Admission Delay (`admission_delay_sec`)**: $t_{\text{job\_created}} - t_{\text{admitted}}$. Measures Kubernetes API `Job` creation latency.
3. **Pod Schedule & Startup Delay (`pod_schedule_delay_sec`)**: $t_{\text{container\_started}} - t_{\text{job\_created}}$. Measures `job-controller` + `kube-scheduler` placement + container startup latency on the GKE node.
4. **Actual Execution Duration (`actual_duration_sec`)**: $t_{\text{finished}} - t_{\text{container\_started}}$.
5. **End-to-End Duration (`end_to_end_duration_sec`)**: $t_{\text{finished}} - t_{\text{submitted}}$.
6. **Makespan (`makespan_sec`)**: $\max_i(t_{\text{finished}, i}) - \min_i(t_{\text{submitted}, i})$.
7. **Jain's Fairness Index (`jains_fairness_index`)**:
   $$\mathcal{J}(x_1, \dots, x_n) = \frac{\left(\sum_{i=1}^n x_i\right)^2}{n \sum_{i=1}^n x_i^2} \in (0, 1]$$
   computed over normalized turnaround ratios $x_i = \frac{\text{ActualDuration}_i}{\text{EndToEndDuration}_i}$.

---

## 3. Sample Size Limitations on Tail Percentiles (`p95`, `p99`)

Every `RunSummaryMetrics` object includes `"percentiles_meaningful": true|false` and `"sample_limitation_note"`:
- When $n < 20$ (for example, bounded live GKE runs with $n=6$ or $n=8$), empirical `p95` and `p99` order statistics collapse to the single maximum value ($\max_i$) and must not be over-interpreted as asymptotic tail estimates.
- Use $n \ge 25$ (`25, 50, 100`) runs when comparing `p95` and `p99` tail latencies.
