package observability

import (
	"math"
	"net/http"

	"github.com/devenes/gke-ai-workload-scheduler/internal/policies"
	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Recorder encapsulates all low-cardinality Prometheus metrics for the scheduler platform.
type Recorder struct {
	registry               *prometheus.Registry
	TasksSubmitted         *prometheus.CounterVec
	TasksCompleted         *prometheus.CounterVec
	QueueDepth             *prometheus.GaugeVec
	QueueWaitSec           *prometheus.HistogramVec
	AdmissionDelaySec      *prometheus.HistogramVec
	PodScheduleDelaySec    *prometheus.HistogramVec
	ExecutionDurationSec   *prometheus.HistogramVec
	EndToEndDurationSec    *prometheus.HistogramVec
	ThroughputTasksPerMin  *prometheus.GaugeVec
	DeadlineOutcomes       *prometheus.CounterVec
	ActiveConcurrentJobs   *prometheus.GaugeVec
	ActiveCPUBudgetMilli   *prometheus.GaugeVec
	ActiveMemoryBudgetMB   *prometheus.GaugeVec
	PolicyDecisions        *prometheus.CounterVec
	PredictedDurationSec   *prometheus.HistogramVec
	PredictionAbsErrorSec  *prometheus.HistogramVec
	PredictorFallbacks     *prometheus.CounterVec
}

// NewRecorder creates an isolated Prometheus registry and pre-initializes all label combinations.
func NewRecorder() *Recorder {
	reg := prometheus.NewRegistry()
	durationBuckets := []float64{0.25, 0.5, 1.0, 2.0, 3.5, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0}
	delayBuckets := []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0}
	errBuckets := []float64{0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.5, 4.0, 6.0, 10.0}

	r := &Recorder{
		registry: reg,
		TasksSubmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_tasks_submitted_total",
			Help: "Total logical batch tasks submitted to the scheduler.",
		}, []string{"policy", "workload_class", "mode"}),
		TasksCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_tasks_completed_total",
			Help: "Total tasks reaching a terminal state (Succeeded, Failed, TimedOut, Cancelled).",
		}, []string{"policy", "workload_class", "status", "mode"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_queue_depth",
			Help: "Current number of tasks in each lifecycle status.",
		}, []string{"status"}),
		QueueWaitSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_queue_wait_seconds",
			Help:    "Time tasks spent waiting in queue prior to capacity admission.",
			Buckets: durationBuckets,
		}, []string{"policy", "mode"}),
		AdmissionDelaySec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_admission_delay_seconds",
			Help:    "Delay between scheduler capacity admission and Kubernetes Job creation.",
			Buckets: delayBuckets,
		}, []string{"policy", "mode"}),
		PodScheduleDelaySec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_pod_schedule_delay_seconds",
			Help:    "Delay between Kubernetes Job creation and container execution start.",
			Buckets: delayBuckets,
		}, []string{"policy", "mode"}),
		ExecutionDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_execution_duration_seconds",
			Help:    "Observed workload execution duration in seconds.",
			Buckets: durationBuckets,
		}, []string{"policy", "workload_class", "mode"}),
		EndToEndDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_end_to_end_duration_seconds",
			Help:    "End-to-end completion duration from submission to finish.",
			Buckets: durationBuckets,
		}, []string{"policy", "mode"}),
		ThroughputTasksPerMin: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_throughput_tasks_per_minute",
			Help: "Measured batch completion throughput in tasks per minute.",
		}, []string{"policy", "mode"}),
		DeadlineOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_deadline_outcomes_total",
			Help: "Total deadline-sensitive tasks that met or missed their completion deadline.",
		}, []string{"policy", "outcome", "mode"}),
		ActiveConcurrentJobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_active_concurrent_jobs",
			Help: "Currently active admitted/running Jobs.",
		}, []string{"mode"}),
		ActiveCPUBudgetMilli: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_active_cpu_budget_milli",
			Help: "Currently allocated CPU requests in millicores across active Jobs.",
		}, []string{"mode"}),
		ActiveMemoryBudgetMB: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_active_memory_budget_mb",
			Help: "Currently allocated memory requests in MiB across active Jobs.",
		}, []string{"mode"}),
		PolicyDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_policy_decisions_total",
			Help: "Total scheduling ranking and dispatch decisions made per policy.",
		}, []string{"policy", "mode"}),
		PredictedDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_predicted_duration_seconds",
			Help:    "Pre-execution runtime durations predicted by the ML predictor.",
			Buckets: durationBuckets,
		}, []string{"policy", "workload_class"}),
		PredictionAbsErrorSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_prediction_abs_error_seconds",
			Help:    "Absolute error |predicted - actual| in seconds for completed tasks.",
			Buckets: errBuckets,
		}, []string{"policy", "workload_class", "mode"}),
		PredictorFallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_predictor_fallbacks_total",
			Help: "Total number of times the scheduler used the deterministic fallback estimator.",
		}, []string{"policy", "reason"}),
	}

	reg.MustRegister(
		r.TasksSubmitted,
		r.TasksCompleted,
		r.QueueDepth,
		r.QueueWaitSec,
		r.AdmissionDelaySec,
		r.PodScheduleDelaySec,
		r.ExecutionDurationSec,
		r.EndToEndDurationSec,
		r.ThroughputTasksPerMin,
		r.DeadlineOutcomes,
		r.ActiveConcurrentJobs,
		r.ActiveCPUBudgetMilli,
		r.ActiveMemoryBudgetMB,
		r.PolicyDecisions,
		r.PredictedDurationSec,
		r.PredictionAbsErrorSec,
		r.PredictorFallbacks,
	)

	// Pre-initialize label series so /metrics and Cloud Monitoring immediately see zeroed time series.
	modes := []string{"simulation", "live_gke"}
	statuses := []workload.TaskStatus{
		workload.StatusPending,
		workload.StatusRanked,
		workload.StatusSubmitted,
		workload.StatusAdmitted,
		workload.StatusRunning,
		workload.StatusSucceeded,
		workload.StatusFailed,
		workload.StatusTimedOut,
		workload.StatusCancelled,
	}
	for _, st := range statuses {
		r.QueueDepth.WithLabelValues(string(st)).Set(0)
	}
	for _, m := range modes {
		r.ActiveConcurrentJobs.WithLabelValues(m).Set(0)
		r.ActiveCPUBudgetMilli.WithLabelValues(m).Set(0)
		r.ActiveMemoryBudgetMB.WithLabelValues(m).Set(0)
		for _, p := range policies.AllPolicies {
			pStr := string(p)
			r.PolicyDecisions.WithLabelValues(pStr, m).Add(0)
			r.ThroughputTasksPerMin.WithLabelValues(pStr, m).Set(0)
			r.DeadlineOutcomes.WithLabelValues(pStr, "met", m).Add(0)
			r.DeadlineOutcomes.WithLabelValues(pStr, "missed", m).Add(0)
			for _, cls := range workload.AllWorkloadClasses {
				cStr := string(cls)
				r.TasksSubmitted.WithLabelValues(pStr, cStr, m).Add(0)
				r.TasksCompleted.WithLabelValues(pStr, cStr, string(workload.StatusSucceeded), m).Add(0)
			}
		}
	}

	return r
}

// RecordTaskOutcome updates Prometheus counters and histograms when a task finishes.
func (r *Recorder) RecordTaskOutcome(t workload.Task, mode string) {
	p := t.PolicyUsed
	if p == "" {
		p = "fifo"
	}
	cls := string(t.WorkloadClass)
	r.TasksCompleted.WithLabelValues(p, cls, string(t.Status), mode).Inc()
	if t.QueueWaitSec >= 0 {
		r.QueueWaitSec.WithLabelValues(p, mode).Observe(t.QueueWaitSec)
	}
	if t.AdmissionDelaySec >= 0 {
		r.AdmissionDelaySec.WithLabelValues(p, mode).Observe(t.AdmissionDelaySec)
	}
	if t.PodScheduleDelaySec >= 0 {
		r.PodScheduleDelaySec.WithLabelValues(p, mode).Observe(t.PodScheduleDelaySec)
	}
	if t.ActualDurationSec > 0 {
		r.ExecutionDurationSec.WithLabelValues(p, cls, mode).Observe(t.ActualDurationSec)
	}
	if t.EndToEndDurationSec > 0 {
		r.EndToEndDurationSec.WithLabelValues(p, mode).Observe(t.EndToEndDurationSec)
	}
	if t.PredictedDurationSec > 0 && t.ActualDurationSec > 0 {
		r.PredictionAbsErrorSec.WithLabelValues(p, cls, mode).Observe(math.Abs(t.PredictedDurationSec - t.ActualDurationSec))
	}
	if t.HasDeadline {
		if t.DeadlineMet {
			r.DeadlineOutcomes.WithLabelValues(p, "met", mode).Inc()
		} else if t.DeadlineMissed {
			r.DeadlineOutcomes.WithLabelValues(p, "missed", mode).Inc()
		}
	}
}

// UpdateQueueGauges refreshes the scheduler_queue_depth gauge from Store.StatusCounts().
func (r *Recorder) UpdateQueueGauges(counts map[workload.TaskStatus]int) {
	for st, c := range counts {
		r.QueueDepth.WithLabelValues(string(st)).Set(float64(c))
	}
}

// Handler returns the HTTP handler for /metrics.
func (r *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}
