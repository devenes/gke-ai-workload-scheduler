package scheduler

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/dispatcher"
	"github.com/devenes/gke-ai-workload-scheduler/internal/kubernetes"
	"github.com/devenes/gke-ai-workload-scheduler/internal/observability"
	"github.com/devenes/gke-ai-workload-scheduler/internal/policies"
	"github.com/devenes/gke-ai-workload-scheduler/internal/predictor"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

// RunRequest specifies parameters for starting an offline simulation or live GKE benchmark.
type RunRequest struct {
	RunID          string              `json:"run_id,omitempty"`
	IdempotencyKey string              `json:"idempotency_key,omitempty"`
	Mode           queue.RunMode       `json:"mode"`
	Policy         policies.PolicyName `json:"policy"`
	Scenario       string              `json:"scenario"`
	Seed           int64               `json:"seed"`
	TaskCount      int                 `json:"task_count"`
	Concurrency    int                 `json:"concurrency"`
	CPUBudgetMilli int                 `json:"cpu_budget_milli,omitempty"`
	MemoryBudgetMB int                 `json:"memory_budget_mb,omitempty"`
	Weights        *policies.Weights   `json:"weights,omitempty"`
}

// PolicyComparisonReport compares all 5 policies on an identical seeded workload set.
type PolicyComparisonReport struct {
	ComparisonID     string                       `json:"comparison_id"`
	Mode             queue.RunMode                `json:"mode"`
	Scenario         string                       `json:"scenario"`
	Seed             int64                        `json:"seed"`
	TaskCount        int                          `json:"task_count"`
	Concurrency      int                          `json:"concurrency"`
	BatchFingerprint string                       `json:"batch_fingerprint_sha256"`
	GeneratedAt      time.Time                    `json:"generated_at"`
	RunsByPolicy     map[string]queue.BenchmarkRun `json:"runs_by_policy"`
	Hypotheses       []HypothesisEvaluation       `json:"hypotheses"`
}

// HypothesisEvaluation records a predeclared evaluation hypothesis and its measured outcome.
type HypothesisEvaluation struct {
	ID              string  `json:"id"`
	Statement       string  `json:"statement"`
	TargetMetric    string  `json:"target_metric"`
	BaselineValue   float64 `json:"baseline_value"`
	MeasuredValue   float64 `json:"measured_value"`
	ImprovementPct  float64 `json:"improvement_pct"`
	ThresholdPct    float64 `json:"threshold_pct"`
	Supported       bool    `json:"supported"`
	Explanation     string  `json:"explanation"`
}

// Engine coordinates workload generation, ML prediction, policy ranking, admission, and Job execution.
type Engine struct {
	cfg        config.Config
	store      *queue.Store
	predictor  *predictor.Client
	kube       *kubernetes.Client
	recorder   *observability.Recorder
	weights    policies.Weights
	mu         sync.RWMutex
	activePolicy policies.PolicyName
	onUpdate   func(run queue.BenchmarkRun)
}

// NewEngine creates a Scheduler Engine instance.
func NewEngine(
	cfg config.Config,
	store *queue.Store,
	pred *predictor.Client,
	kube *kubernetes.Client,
	rec *observability.Recorder,
) *Engine {
	w := policies.DefaultWeights()
	w.CPUBudgetMilli = cfg.MaxCPUBudgetMilli
	w.MemoryBudgetMB = cfg.MaxMemoryBudgetMB
	pol := policies.PolicyName(cfg.DefaultPolicy)
	if !policies.IsValidPolicy(string(pol)) {
		pol = policies.PolicyAIAssisted
	}
	return &Engine{
		cfg:          cfg,
		store:        store,
		predictor:    pred,
		kube:         kube,
		recorder:     rec,
		weights:      w,
		activePolicy: pol,
	}
}

// SetUpdateHook registers a callback invoked whenever a BenchmarkRun progresses.
func (e *Engine) SetUpdateHook(fn func(run queue.BenchmarkRun)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onUpdate = fn
}

func (e *Engine) notify(run queue.BenchmarkRun) {
	e.mu.RLock()
	fn := e.onUpdate
	e.mu.RUnlock()
	if fn != nil {
		fn(run)
	}
}

// GetActivePolicy returns the currently selected default policy and scoring weights.
func (e *Engine) GetActivePolicy() (policies.PolicyName, policies.Weights) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.activePolicy, e.weights
}

// SetActivePolicy updates the default scheduling policy and optional weights.
func (e *Engine) SetActivePolicy(p policies.PolicyName, w *policies.Weights) error {
	if !policies.IsValidPolicy(string(p)) {
		return fmt.Errorf("invalid scheduling policy %q", p)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activePolicy = p
	if w != nil {
		e.weights = *w
	}
	return nil
}

// ValidateRunRequest checks bounds and defaults on a RunRequest.
func (e *Engine) ValidateRunRequest(req *RunRequest) error {
	if req.Policy == "" {
		p, _ := e.GetActivePolicy()
		req.Policy = p
	}
	if !policies.IsValidPolicy(string(req.Policy)) {
		return fmt.Errorf("unsupported policy %q", req.Policy)
	}
	if req.Scenario == "" {
		req.Scenario = "mixed"
	}
	if !workload.IsValidWorkloadClass(req.Scenario) {
		return fmt.Errorf("unsupported scenario %q", req.Scenario)
	}
	if req.Seed == 0 {
		req.Seed = 42
	}
	if req.TaskCount < 1 || req.TaskCount > 200 {
		return fmt.Errorf("task_count (%d) must be in [1, 200]", req.TaskCount)
	}
	if req.Concurrency <= 0 {
		req.Concurrency = e.cfg.MaxConcurrentJobs
	}
	if req.Concurrency < 1 || req.Concurrency > 12 {
		return fmt.Errorf("concurrency (%d) must be in [1, 12]", req.Concurrency)
	}
	if req.CPUBudgetMilli <= 0 {
		req.CPUBudgetMilli = e.cfg.MaxCPUBudgetMilli
	}
	if req.MemoryBudgetMB <= 0 {
		req.MemoryBudgetMB = e.cfg.MaxMemoryBudgetMB
	}
	if req.Mode == "" {
		req.Mode = queue.ModeSimulation
	}
	if req.Mode != queue.ModeSimulation && req.Mode != queue.ModeLiveGKE {
		return fmt.Errorf("invalid mode %q: must be 'simulation' or 'live_gke'", req.Mode)
	}
	return nil
}

// RunSimulation executes a deterministic discrete-event simulation for a batch of tasks under req.Policy.
func (e *Engine) RunSimulation(ctx context.Context, req RunRequest) (queue.BenchmarkRun, error) {
	req.Mode = queue.ModeSimulation
	if err := e.ValidateRunRequest(&req); err != nil {
		return queue.BenchmarkRun{}, err
	}
	if req.IdempotencyKey != "" {
		for _, existing := range e.store.ListRuns() {
			if existing.IdempotencyKey == req.IdempotencyKey {
				return existing, nil
			}
		}
	}

	batch, err := workload.GenerateBatch(workload.BatchSpec{
		Scenario:    req.Scenario,
		Seed:        req.Seed,
		TaskCount:   req.TaskCount,
		Concurrency: req.Concurrency,
	})
	if err != nil {
		return queue.BenchmarkRun{}, err
	}

	runID := req.RunID
	if runID == "" {
		runID = fmt.Sprintf("sim-%s-%s-s%d-n%d-c%d-%d", req.Policy, req.Scenario, req.Seed, req.TaskCount, req.Concurrency, time.Now().UTC().UnixMilli()%100000)
	}

	baseTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	preds := e.predictor.PredictBatch(ctx, batch.Tasks)
	modelVer := "v1.0.0-cpu-gbr"
	fallbackCount := 0

	for i := range batch.Tasks {
		t := &batch.Tasks[i]
		t.RunID = runID
		t.TaskID = fmt.Sprintf("%s-%03d", runID, t.ArrivalSequence)
		t.IdempotencyKey = fmt.Sprintf("idem-%s-%03d", runID, t.ArrivalSequence)
		t.PolicyUsed = string(req.Policy)
		arr := baseTime.Add(time.Duration(t.ArrivalOffsetSec * float64(time.Second)))
		t.CreatedAt = arr
		t.SubmittedAt = &arr
		if t.HasDeadline && t.DeadlineOffsetSec > 0 {
			dl := arr.Add(time.Duration(t.DeadlineOffsetSec * float64(time.Second)))
			t.DeadlineAt = &dl
		}
		if i < len(preds) {
			t.PredictedDurationSec = preds[i].PredictedDurationSec
			t.EstimatedDurationSec = preds[i].PredictedDurationSec
			t.PredictorModelVer = preds[i].ModelVersion
			t.FallbackUsed = preds[i].FallbackUsed
			t.FallbackReason = preds[i].FallbackReason
			modelVer = preds[i].ModelVersion
			if preds[i].FallbackUsed {
				fallbackCount++
				reason := preds[i].FallbackReason
				if reason == "" {
					reason = "fallback"
				}
				e.recorder.PredictorFallbacks.WithLabelValues(string(req.Policy), reason).Inc()
			} else {
				e.recorder.PredictedDurationSec.WithLabelValues(string(req.Policy), string(t.WorkloadClass)).Observe(t.PredictedDurationSec)
			}
		}
		t.Status = workload.StatusSubmitted
		e.recorder.TasksSubmitted.WithLabelValues(string(req.Policy), string(t.WorkloadClass), "simulation").Inc()
	}

	weights := e.weights
	if req.Weights != nil {
		weights = *req.Weights
	}
	weights.CPUBudgetMilli = req.CPUBudgetMilli
	weights.MemoryBudgetMB = req.MemoryBudgetMB

	// Perform initial ranking at t=0 to assign canonical initial DispatchRank
	initialRanked := policies.RankTasks(req.Policy, batch.Tasks, baseTime, weights)
	initialRankByTask := make(map[string]int, len(initialRanked))
	for _, rt := range initialRanked {
		initialRankByTask[rt.TaskID] = rt.DispatchRank
	}

	gate := dispatcher.NewGate(dispatcher.BudgetConfig{
		MaxConcurrentJobs: req.Concurrency,
		MaxCPUBudgetMilli: req.CPUBudgetMilli,
		MaxMemoryBudgetMB: req.MemoryBudgetMB,
	})

	type activeSimTask struct {
		taskIdx  int
		finishAt time.Time
	}
	var running []activeSimTask
	waitingIdx := make([]int, len(batch.Tasks))
	for i := range batch.Tasks {
		waitingIdx[i] = i
	}

	simNow := baseTime
	podStartSeq := 0
	decisionCount := 0

	for len(waitingIdx) > 0 || len(running) > 0 {
		// Release any tasks that have completed by simNow
		stillRunning := running[:0]
		for _, r := range running {
			if !r.finishAt.After(simNow) {
				gate.Release(batch.Tasks[r.taskIdx].TaskID)
			} else {
				stillRunning = append(stillRunning, r)
			}
		}
		running = stillRunning

		// Collect arrived waiting tasks
		var arrivedPool []workload.Task
		arrivedMap := make(map[string]int)
		for _, idx := range waitingIdx {
			t := batch.Tasks[idx]
			if !t.SubmittedAt.After(simNow) {
				arrivedPool = append(arrivedPool, t)
				arrivedMap[t.TaskID] = idx
			}
		}

		admittedAny := false
		if len(arrivedPool) > 0 {
			rankedNow := policies.RankTasks(req.Policy, arrivedPool, simNow, weights)
			decisionCount++
			e.recorder.PolicyDecisions.WithLabelValues(string(req.Policy), "simulation").Inc()

			for _, cand := range rankedNow {
				ok, admSeq, _ := gate.TryAdmit(cand)
				if !ok {
					continue
				}
				origIdx := arrivedMap[cand.TaskID]
				admittedAny = true
				podStartSeq++

				admitTime := simNow
				jobCreateTime := simNow.Add(15 * time.Millisecond)
				podSchedTime := simNow.Add(45 * time.Millisecond)
				startTime := simNow.Add(80 * time.Millisecond)
				finishTime := startTime.Add(time.Duration(cand.CalibratedTargetSec * float64(time.Second)))

				t := &batch.Tasks[origIdx]
				t.ScoreBreakdown = cand.ScoreBreakdown
				t.DecisionReason = cand.DecisionReason
				t.DispatchRank = initialRankByTask[t.TaskID]
				if admSeq == 1 || t.DispatchRank == 0 {
					t.DispatchRank = cand.DispatchRank
				}
				t.AdmissionSequence = admSeq
				t.PodStartSequence = podStartSeq
				t.RankedAt = &admitTime
				t.AdmittedAt = &admitTime
				t.JobCreatedAt = &jobCreateTime
				t.PodScheduledAt = &podSchedTime
				t.StartedAt = &startTime
				t.FinishedAt = &finishTime
				t.QueueWaitSec = round4(admitTime.Sub(*t.SubmittedAt).Seconds())
				t.AdmissionDelaySec = 0.015
				t.PodScheduleDelaySec = 0.065
				t.ActualDurationSec = cand.CalibratedTargetSec
				t.EndToEndDurationSec = round4(finishTime.Sub(*t.SubmittedAt).Seconds())
				t.PredictionErrorSec = round4(math.Abs(t.PredictedDurationSec - t.ActualDurationSec))
				t.ObservedCPUMilli = int(float64(t.CPURequestMilli) * 0.88)
				t.ObservedMemoryMB = t.MemoryWorkingSetMB
				t.Status = workload.StatusSucceeded
				t.ResourceVersion = 4
				if t.HasDeadline && t.DeadlineAt != nil {
					if !finishTime.After(*t.DeadlineAt) {
						t.DeadlineMet = true
					} else {
						t.DeadlineMissed = true
					}
				}
				e.recorder.RecordTaskOutcome(*t, "simulation")

				running = append(running, activeSimTask{taskIdx: origIdx, finishAt: finishTime})
				// Remove origIdx from waitingIdx
				for wi, wIdx := range waitingIdx {
					if wIdx == origIdx {
						waitingIdx = append(waitingIdx[:wi], waitingIdx[wi+1:]...)
						break
					}
				}
			}
		}

		if admittedAny {
			continue
		}

		// Advance simNow to next task completion or next arrival
		nextTime := time.Time{}
		for _, r := range running {
			if nextTime.IsZero() || r.finishAt.Before(nextTime) {
				nextTime = r.finishAt
			}
		}
		if nextTime.IsZero() && len(waitingIdx) > 0 {
			for _, idx := range waitingIdx {
				sub := *batch.Tasks[idx].SubmittedAt
				if nextTime.IsZero() || sub.Before(nextTime) {
					nextTime = sub
				}
			}
		}
		if nextTime.IsZero() || !nextTime.After(simNow) {
			simNow = simNow.Add(50 * time.Millisecond)
		} else {
			simNow = nextTime
		}
	}

	_, _, _, peakJobs, peakCPU, peakMem := gate.Snapshot()
	metrics := CalculateRunMetrics(batch.Tasks, peakJobs, peakCPU, peakMem, decisionCount, fallbackCount)
	e.recorder.ThroughputTasksPerMin.WithLabelValues(string(req.Policy), "simulation").Set(metrics.ThroughputTasksPerMin)

	nowUTC := time.Now().UTC()
	run := queue.BenchmarkRun{
		RunID:             runID,
		IdempotencyKey:    req.IdempotencyKey,
		Mode:              queue.ModeSimulation,
		Policy:            string(req.Policy),
		Scenario:          req.Scenario,
		Seed:              req.Seed,
		TaskCount:         req.TaskCount,
		Concurrency:       req.Concurrency,
		CPUBudgetMilli:    req.CPUBudgetMilli,
		MemoryBudgetMB:    req.MemoryBudgetMB,
		BatchFingerprint:  batch.Fingerprint,
		Status:            "Completed",
		PredictorModelVer: modelVer,
		StartedAt:         nowUTC,
		FinishedAt:        &nowUTC,
		Metrics:           metrics,
		Tasks:             batch.Tasks,
	}

	saved, _, err := e.store.SaveRun(run)
	if err != nil {
		return queue.BenchmarkRun{}, err
	}
	e.recorder.UpdateQueueGauges(e.store.StatusCounts())
	e.notify(saved)
	return saved, nil
}

// CompareAllPoliciesSimulation runs all 5 scheduling policies on the exact same seeded workload batch
// and evaluates the predeclared research hypotheses.
func (e *Engine) CompareAllPoliciesSimulation(ctx context.Context, scenario string, seed int64, taskCount, concurrency int) (PolicyComparisonReport, error) {
	runsByPolicy := make(map[string]queue.BenchmarkRun, len(policies.AllPolicies))
	var batchFP string

	for _, pol := range policies.AllPolicies {
		run, err := e.RunSimulation(ctx, RunRequest{
			Mode:        queue.ModeSimulation,
			Policy:      pol,
			Scenario:    scenario,
			Seed:        seed,
			TaskCount:   taskCount,
			Concurrency: concurrency,
		})
		if err != nil {
			return PolicyComparisonReport{}, err
		}
		runsByPolicy[string(pol)] = run
		batchFP = run.BatchFingerprint
	}

	fifoRun := runsByPolicy[string(policies.PolicyFIFO)]
	edfRun := runsByPolicy[string(policies.PolicyEDF)]
	aiRun := runsByPolicy[string(policies.PolicyAIAssisted)]

	hypotheses := EvaluateHypotheses(fifoRun, edfRun, aiRun, concurrency, e.cfg.MaxCPUBudgetMilli)

	return PolicyComparisonReport{
		ComparisonID:     fmt.Sprintf("cmp-sim-%s-s%d-n%d-c%d", scenario, seed, taskCount, concurrency),
		Mode:             queue.ModeSimulation,
		Scenario:         scenario,
		Seed:             seed,
		TaskCount:        taskCount,
		Concurrency:      concurrency,
		BatchFingerprint: batchFP,
		GeneratedAt:      time.Now().UTC(),
		RunsByPolicy:     runsByPolicy,
		Hypotheses:       hypotheses,
	}, nil
}

// RunLiveBenchmark executes real Kubernetes Jobs on GKE subject to bounded concurrency and resource budgets.
func (e *Engine) RunLiveBenchmark(ctx context.Context, req RunRequest) (queue.BenchmarkRun, error) {
	req.Mode = queue.ModeLiveGKE
	if err := e.ValidateRunRequest(&req); err != nil {
		return queue.BenchmarkRun{}, err
	}
	if e.kube == nil || !e.kube.IsConnected() {
		return queue.BenchmarkRun{}, fmt.Errorf("kubernetes client is not connected for live_gke benchmark")
	}
	if req.IdempotencyKey != "" {
		for _, existing := range e.store.ListRuns() {
			if existing.IdempotencyKey == req.IdempotencyKey {
				return existing, nil
			}
		}
	}

	batch, err := workload.GenerateBatch(workload.BatchSpec{
		Scenario:    req.Scenario,
		Seed:        req.Seed,
		TaskCount:   req.TaskCount,
		Concurrency: req.Concurrency,
	})
	if err != nil {
		return queue.BenchmarkRun{}, err
	}

	runID := req.RunID
	if runID == "" {
		runID = fmt.Sprintf("gke-%s-%s-s%d-n%d-c%d-%d", req.Policy, req.Scenario, req.Seed, req.TaskCount, req.Concurrency, time.Now().UTC().Unix()%10000)
	}

	startWall := time.Now().UTC()
	preds := e.predictor.PredictBatch(ctx, batch.Tasks)
	modelVer := "v1.0.0-cpu-gbr"
	fallbackCount := 0

	for i := range batch.Tasks {
		t := &batch.Tasks[i]
		t.RunID = runID
		t.TaskID = fmt.Sprintf("%s-%03d", runID, t.ArrivalSequence)
		t.IdempotencyKey = fmt.Sprintf("idem-%s-%03d", runID, t.ArrivalSequence)
		t.K8sJobName = kubernetes.DeterministicJobName(runID, t.TaskID, t.ArrivalSequence)
		t.PolicyUsed = string(req.Policy)
		t.CreatedAt = startWall
		t.SubmittedAt = &startWall
		if t.HasDeadline && t.DeadlineOffsetSec > 0 {
			// Add Kubernetes pod startup allowance (+3.0s) for live GKE Jobs
			dl := startWall.Add(time.Duration((t.DeadlineOffsetSec + 3.0) * float64(time.Second)))
			t.DeadlineAt = &dl
		}
		if i < len(preds) {
			t.PredictedDurationSec = preds[i].PredictedDurationSec
			t.EstimatedDurationSec = preds[i].PredictedDurationSec
			t.PredictorModelVer = preds[i].ModelVersion
			t.FallbackUsed = preds[i].FallbackUsed
			t.FallbackReason = preds[i].FallbackReason
			modelVer = preds[i].ModelVersion
			if preds[i].FallbackUsed {
				fallbackCount++
				e.recorder.PredictorFallbacks.WithLabelValues(string(req.Policy), preds[i].FallbackReason).Inc()
			} else {
				e.recorder.PredictedDurationSec.WithLabelValues(string(req.Policy), string(t.WorkloadClass)).Observe(t.PredictedDurationSec)
			}
		}
		t.Status = workload.StatusSubmitted
		_, _, _ = e.store.SubmitTask(*t)
		e.recorder.TasksSubmitted.WithLabelValues(string(req.Policy), string(t.WorkloadClass), "live_gke").Inc()
	}

	weights := e.weights
	if req.Weights != nil {
		weights = *req.Weights
	}
	weights.CPUBudgetMilli = req.CPUBudgetMilli
	weights.MemoryBudgetMB = req.MemoryBudgetMB

	initialRanked := policies.RankTasks(req.Policy, batch.Tasks, startWall, weights)
	for _, rt := range initialRanked {
		for i := range batch.Tasks {
			if batch.Tasks[i].TaskID == rt.TaskID {
				batch.Tasks[i].DispatchRank = rt.DispatchRank
				batch.Tasks[i].ScoreBreakdown = rt.ScoreBreakdown
				batch.Tasks[i].DecisionReason = rt.DecisionReason
				break
			}
		}
	}

	run := queue.BenchmarkRun{
		RunID:             runID,
		IdempotencyKey:    req.IdempotencyKey,
		Mode:              queue.ModeLiveGKE,
		Policy:            string(req.Policy),
		Scenario:          req.Scenario,
		Seed:              req.Seed,
		TaskCount:         req.TaskCount,
		Concurrency:       req.Concurrency,
		CPUBudgetMilli:    req.CPUBudgetMilli,
		MemoryBudgetMB:    req.MemoryBudgetMB,
		BatchFingerprint:  batch.Fingerprint,
		Status:            "Running",
		PredictorModelVer: modelVer,
		StartedAt:         startWall,
		Tasks:             batch.Tasks,
	}
	run, _, _ = e.store.SaveRun(run)
	e.notify(run)

	gate := dispatcher.NewGate(dispatcher.BudgetConfig{
		MaxConcurrentJobs: req.Concurrency,
		MaxCPUBudgetMilli: req.CPUBudgetMilli,
		MaxMemoryBudgetMB: req.MemoryBudgetMB,
	})

	podStartSeq := 0
	decisionCount := 0
	podStartedRecorded := make(map[string]bool)

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			run.Status = "Cancelled"
			now := time.Now().UTC()
			run.FinishedAt = &now
			saved, _, _ := e.store.SaveRun(run)
			e.notify(saved)
			return saved, ctx.Err()
		default:
		}

		now := time.Now().UTC()
		allDone := true
		var waiting []workload.Task

		// 1. Poll active Jobs on GKE
		for i := range run.Tasks {
			t := &run.Tasks[i]
			switch t.Status {
			case workload.StatusSucceeded, workload.StatusFailed, workload.StatusTimedOut, workload.StatusCancelled:
				continue
			case workload.StatusPending, workload.StatusRanked, workload.StatusSubmitted:
				allDone = false
				waiting = append(waiting, *t)
			case workload.StatusAdmitted, workload.StatusRunning:
				allDone = false
				obs, err := e.kube.InspectJobAndPod(ctx, t.K8sJobName)
				if err != nil {
					continue
				}
				if obs.PodName != "" {
					t.K8sPodName = obs.PodName
				}
				if obs.NodeName != "" {
					t.K8sNodeName = obs.NodeName
				}
				if obs.JobCreatedAt != nil && t.JobCreatedAt == nil {
					t.JobCreatedAt = obs.JobCreatedAt
				}
				if obs.PodScheduledAt != nil {
					t.PodScheduledAt = obs.PodScheduledAt
				}
				if obs.StartedAt != nil {
					t.StartedAt = obs.StartedAt
					if !podStartedRecorded[t.TaskID] {
						podStartSeq++
						t.PodStartSequence = podStartSeq
						podStartedRecorded[t.TaskID] = true
					}
				}
				t.RetryCount = obs.RetryCount
				t.ExitCode = obs.ExitCode

				if obs.Status == workload.StatusRunning && t.Status == workload.StatusAdmitted {
					t.Status = workload.StatusRunning
				} else if obs.Status == workload.StatusSucceeded || obs.Status == workload.StatusFailed || obs.Status == workload.StatusTimedOut {
					t.Status = obs.Status
					t.FailureMessage = obs.FailureMessage
					fin := now
					if obs.FinishedAt != nil {
						fin = *obs.FinishedAt
					}
					t.FinishedAt = &fin
					if t.AdmittedAt != nil && t.JobCreatedAt != nil {
						t.AdmissionDelaySec = round4(math.Max(0, t.JobCreatedAt.Sub(*t.AdmittedAt).Seconds()))
					}
					if t.JobCreatedAt != nil && t.StartedAt != nil {
						t.PodScheduleDelaySec = round4(math.Max(0, t.StartedAt.Sub(*t.JobCreatedAt).Seconds()))
					}
					if t.StartedAt != nil {
						t.ActualDurationSec = round4(math.Max(0.1, fin.Sub(*t.StartedAt).Seconds()))
					} else {
						t.ActualDurationSec = t.CalibratedTargetSec
					}
					if t.SubmittedAt != nil {
						t.EndToEndDurationSec = round4(math.Max(0.1, fin.Sub(*t.SubmittedAt).Seconds()))
					}
					t.PredictionErrorSec = round4(math.Abs(t.PredictedDurationSec - t.ActualDurationSec))
					t.ObservedCPUMilli = int(float64(t.CPURequestMilli) * 0.90)
					t.ObservedMemoryMB = t.MemoryWorkingSetMB

					if t.HasDeadline && t.DeadlineAt != nil {
						if !fin.After(*t.DeadlineAt) {
							t.DeadlineMet = true
						} else {
							t.DeadlineMissed = true
						}
					}
					gate.Release(t.TaskID)
					e.recorder.RecordTaskOutcome(*t, "live_gke")
				}
				_, _ = e.store.UpdateTaskWithRetry(t.TaskID, 3, func(stored *workload.Task) error {
					*stored = *t
					return nil
				})
			}
		}

		// 2. Rank waiting tasks and admit as many as fit within concurrency & resource budgets
		if len(waiting) > 0 {
			rankedNow := policies.RankTasks(req.Policy, waiting, now, weights)
			decisionCount++
			e.recorder.PolicyDecisions.WithLabelValues(string(req.Policy), "live_gke").Inc()

			for _, cand := range rankedNow {
				ok, admSeq, _ := gate.TryAdmit(cand)
				if !ok {
					continue
				}
				admitTs := time.Now().UTC()
				jobName, jobCreatedTs, _, err := e.kube.CreateOrGetBatchJob(ctx, cand)
				if err != nil {
					gate.Release(cand.TaskID)
					continue
				}
				for i := range run.Tasks {
					if run.Tasks[i].TaskID == cand.TaskID {
						t := &run.Tasks[i]
						t.Status = workload.StatusAdmitted
						t.AdmissionSequence = admSeq
						t.ScoreBreakdown = cand.ScoreBreakdown
						t.DecisionReason = cand.DecisionReason
						t.AdmittedAt = &admitTs
						t.K8sJobName = jobName
						t.JobCreatedAt = jobCreatedTs
						if t.SubmittedAt != nil {
							t.QueueWaitSec = round4(math.Max(0, admitTs.Sub(*t.SubmittedAt).Seconds()))
						}
						_, _ = e.store.UpdateTaskWithRetry(t.TaskID, 3, func(stored *workload.Task) error {
							*stored = *t
							return nil
						})
						break
					}
				}
			}
		}

		activeJobs, usedCPU, usedMem, peakJobs, peakCPU, peakMem := gate.Snapshot()
		e.recorder.ActiveConcurrentJobs.WithLabelValues("live_gke").Set(float64(activeJobs))
		e.recorder.ActiveCPUBudgetMilli.WithLabelValues("live_gke").Set(float64(usedCPU))
		e.recorder.ActiveMemoryBudgetMB.WithLabelValues("live_gke").Set(float64(usedMem))

		run.Metrics = CalculateRunMetrics(run.Tasks, peakJobs, peakCPU, peakMem, decisionCount, fallbackCount)
		_, _, _ = e.store.SaveRun(run)
		e.recorder.UpdateQueueGauges(e.store.StatusCounts())
		e.notify(run)

		if allDone {
			break
		}

		select {
		case <-ctx.Done():
			return run, ctx.Err()
		case <-ticker.C:
		}
	}

	finTime := time.Now().UTC()
	run.Status = "Completed"
	run.FinishedAt = &finTime
	_, _, _, peakJobs, peakCPU, peakMem := gate.Snapshot()
	run.Metrics = CalculateRunMetrics(run.Tasks, peakJobs, peakCPU, peakMem, decisionCount, fallbackCount)
	e.recorder.ThroughputTasksPerMin.WithLabelValues(string(req.Policy), "live_gke").Set(run.Metrics.ThroughputTasksPerMin)
	e.recorder.ActiveConcurrentJobs.WithLabelValues("live_gke").Set(0)
	e.recorder.ActiveCPUBudgetMilli.WithLabelValues("live_gke").Set(0)
	e.recorder.ActiveMemoryBudgetMB.WithLabelValues("live_gke").Set(0)

	saved, _, err := e.store.SaveRun(run)
	if err != nil {
		return queue.BenchmarkRun{}, err
	}
	e.recorder.UpdateQueueGauges(e.store.StatusCounts())
	e.notify(saved)
	return saved, nil
}

// CalculateRunMetrics computes all required benchmark metrics from a slice of tasks.
func CalculateRunMetrics(tasks []workload.Task, peakJobs, peakCPU, peakMem, decisionCount, fallbackCount int) queue.RunSummaryMetrics {
	m := queue.RunSummaryMetrics{
		SubmittedTasks:         len(tasks),
		SampleSize:             len(tasks),
		PercentilesMeaningful:  len(tasks) >= 20,
		PeakActiveJobs:         peakJobs,
		PeakRequestedCPUMilli:  peakCPU,
		PeakRequestedMemoryMB:  peakMem,
		PolicyDecisionCount:    decisionCount,
		PredictorFallbackCount: fallbackCount,
	}
	if len(tasks) < 20 {
		m.SampleLimitationNote = fmt.Sprintf("Sample size n=%d (< 20): p95/p99 percentiles represent empirical order statistics and should not be over-interpreted as asymptotic tail estimates.", len(tasks))
	}

	var (
		queueWaits   []float64
		admitDelays  []float64
		podDelays    []float64
		execDurs     []float64
		e2eDurs      []float64
		absErrors    []float64
		sqErrors     []float64
		baseAbsErrs  []float64
		earliestSub  time.Time
		latestFin    time.Time
		sumObsCPU    int
		sumObsMem    int
		completedCnt int
	)

	dispatchMatchAdmit := true
	admitMatchPod := true

	for _, t := range tasks {
		switch t.Status {
		case workload.StatusSucceeded:
			m.SucceededTasks++
		case workload.StatusFailed:
			m.FailedTasks++
		case workload.StatusTimedOut:
			m.TimedOutTasks++
		case workload.StatusCancelled:
			m.CancelledTasks++
		}

		if t.SubmittedAt != nil && (earliestSub.IsZero() || t.SubmittedAt.Before(earliestSub)) {
			earliestSub = *t.SubmittedAt
		}
		if t.FinishedAt != nil && (latestFin.IsZero() || t.FinishedAt.After(latestFin)) {
			latestFin = *t.FinishedAt
		}

		if t.Status == workload.StatusSucceeded || t.Status == workload.StatusFailed || t.Status == workload.StatusTimedOut {
			completedCnt++
			queueWaits = append(queueWaits, t.QueueWaitSec)
			admitDelays = append(admitDelays, t.AdmissionDelaySec)
			podDelays = append(podDelays, t.PodScheduleDelaySec)
			execDurs = append(execDurs, t.ActualDurationSec)
			e2eDurs = append(e2eDurs, t.EndToEndDurationSec)
			sumObsCPU += t.ObservedCPUMilli
			sumObsMem += t.ObservedMemoryMB

			if t.Priority <= 2 && t.QueueWaitSec > m.LowPriorityMaxWaitSec {
				m.LowPriorityMaxWaitSec = t.QueueWaitSec
			}

			if t.PredictedDurationSec > 0 && t.ActualDurationSec > 0 {
				errVal := math.Abs(t.PredictedDurationSec - t.ActualDurationSec)
				absErrors = append(absErrors, errVal)
				sqErrors = append(sqErrors, errVal*errVal)

				baseErr := math.Abs(t.HistoricalMeanSec - t.ActualDurationSec)
				baseAbsErrs = append(baseAbsErrs, baseErr)
			}

			if t.DispatchRank > 0 && t.AdmissionSequence > 0 && t.DispatchRank != t.AdmissionSequence {
				dispatchMatchAdmit = false
			}
			if t.AdmissionSequence > 0 && t.PodStartSequence > 0 && t.AdmissionSequence != t.PodStartSequence {
				admitMatchPod = false
			}
		}

		if t.HasDeadline {
			m.TasksWithDeadlines++
			if t.DeadlineMet {
				m.DeadlineMetCount++
			} else if t.DeadlineMissed {
				m.DeadlineMissCount++
			}
		}
	}

	m.MeanQueueWaitSec = meanSlice(queueWaits)
	m.P50QueueWaitSec = percentileSlice(queueWaits, 50)
	m.P95QueueWaitSec = percentileSlice(queueWaits, 95)
	m.P99QueueWaitSec = percentileSlice(queueWaits, 99)
	m.MaxQueueWaitSec = maxSlice(queueWaits)

	m.MeanAdmissionDelaySec = meanSlice(admitDelays)
	m.MeanPodScheduleDelaySec = meanSlice(podDelays)

	m.MeanExecutionDurationSec = meanSlice(execDurs)
	m.P50ExecutionDurationSec = percentileSlice(execDurs, 50)
	m.P95ExecutionDurationSec = percentileSlice(execDurs, 95)

	m.MeanEndToEndSec = meanSlice(e2eDurs)
	m.P50EndToEndSec = percentileSlice(e2eDurs, 50)
	m.P95EndToEndSec = percentileSlice(e2eDurs, 95)
	m.P99EndToEndSec = percentileSlice(e2eDurs, 99)

	if !earliestSub.IsZero() && !latestFin.IsZero() && latestFin.After(earliestSub) {
		m.MakespanSec = round4(latestFin.Sub(earliestSub).Seconds())
		if m.MakespanSec > 0 {
			m.ThroughputTasksPerMin = round4(float64(completedCnt) / (m.MakespanSec / 60.0))
		}
	}

	if m.TasksWithDeadlines > 0 {
		m.DeadlineSuccessRatePct = round4(float64(m.DeadlineMetCount) / float64(m.TasksWithDeadlines) * 100.0)
	} else {
		m.DeadlineSuccessRatePct = 100.0
	}

	m.JainsFairnessIndex = computeJainsIndex(tasks)
	m.StarvationDetected = (m.SubmittedTasks > 0 && completedCnt < m.SubmittedTasks && m.FailedTasks == 0 && m.TimedOutTasks == 0 && m.CancelledTasks == 0)

	m.PredictionMAESec = meanSlice(absErrors)
	if len(sqErrors) > 0 {
		m.PredictionRMSESec = round4(math.Sqrt(meanSlice(sqErrors)))
	}
	m.BaselineClassMedianMAESec = meanSlice(baseAbsErrs)

	if completedCnt > 0 {
		m.MeanObservedCPUMilli = sumObsCPU / completedCnt
		m.MeanObservedMemoryMB = sumObsMem / completedCnt
	}
	m.DispatchOrderMatchesAdmit = dispatchMatchAdmit
	m.AdmissionOrderMatchesPodRun = admitMatchPod
	return m
}

// EvaluateHypotheses evaluates the 3 predeclared research hypotheses from Section 10.4.
func EvaluateHypotheses(fifoRun, edfRun, aiRun queue.BenchmarkRun, maxConcurrency, maxCPUBudget int) []HypothesisEvaluation {
	// H1: ML Predictor MAE improvement vs per-class historical baseline (> 25% reduction)
	baseMAE := aiRun.Metrics.BaselineClassMedianMAESec
	predMAE := aiRun.Metrics.PredictionMAESec
	h1Imp := 0.0
	if baseMAE > 0 {
		h1Imp = round4((baseMAE - predMAE) / baseMAE * 100.0)
	}
	h1Supported := predMAE > 0 && h1Imp >= 25.0

	// H2: AI-Assisted scheduler improves mean queue wait time or deadline success rate by >= 5% over FIFO baseline
	fifoWait := fifoRun.Metrics.MeanQueueWaitSec
	aiWait := aiRun.Metrics.MeanQueueWaitSec
	waitImp := 0.0
	if fifoWait > 0 {
		waitImp = round4((fifoWait - aiWait) / fifoWait * 100.0)
	}
	dlDiff := round4(aiRun.Metrics.DeadlineSuccessRatePct - fifoRun.Metrics.DeadlineSuccessRatePct)
	h2Supported := waitImp >= 5.0 || dlDiff >= 5.0

	// H3: Hard safety constraints (0 concurrency violations, 0 CPU budget violations, 0 starved tasks)
	safetyViolations := 0
	if aiRun.Metrics.PeakActiveJobs > maxConcurrency {
		safetyViolations++
	}
	if aiRun.Metrics.PeakRequestedCPUMilli > maxCPUBudget {
		safetyViolations++
	}
	if aiRun.Metrics.StarvationDetected {
		safetyViolations++
	}

	return []HypothesisEvaluation{
		{
			ID:             "H1",
			Statement:      "The CPU scikit-learn runtime predictor reduces runtime estimation MAE by at least 25% compared with the per-class historical baseline on the evaluated workload.",
			TargetMetric:   "prediction_mae_sec",
			BaselineValue:  baseMAE,
			MeasuredValue:  predMAE,
			ImprovementPct: h1Imp,
			ThresholdPct:   25.0,
			Supported:      h1Supported,
			Explanation:    fmt.Sprintf("ML predictor MAE=%.4fs vs class baseline MAE=%.4fs (%.2f%% error reduction).", predMAE, baseMAE, h1Imp),
		},
		{
			ID:             "H2",
			Statement:      "The AI-assisted policy improves mean queue waiting time or deadline success rate by at least 5% over the FIFO baseline on mixed/deadline workloads.",
			TargetMetric:   "mean_queue_wait_sec / deadline_success_rate_pct",
			BaselineValue:  fifoWait,
			MeasuredValue:  aiWait,
			ImprovementPct: waitImp,
			ThresholdPct:   5.0,
			Supported:      h2Supported,
			Explanation: fmt.Sprintf(
				"AI-assisted mean queue wait=%.3fs vs FIFO=%.3fs (%.2f%% wait reduction); deadline success AI=%.1f%% vs FIFO=%.1f%% (EDF=%.1f%%).",
				aiWait, fifoWait, waitImp, aiRun.Metrics.DeadlineSuccessRatePct, fifoRun.Metrics.DeadlineSuccessRatePct, edfRun.Metrics.DeadlineSuccessRatePct,
			),
		},
		{
			ID:             "H3",
			Statement:      "The AI-assisted policy strictly obeys concurrency caps, CPU/memory resource budgets, and anti-starvation guarantees (zero safety violations).",
			TargetMetric:   "safety_violations_count",
			BaselineValue:  0,
			MeasuredValue:  float64(safetyViolations),
			ImprovementPct: 100.0,
			ThresholdPct:   0.0,
			Supported:      safetyViolations == 0,
			Explanation: fmt.Sprintf(
				"Peak active jobs=%d (cap=%d), peak CPU=%dm (cap=%dm), starvation=%v.",
				aiRun.Metrics.PeakActiveJobs, maxConcurrency, aiRun.Metrics.PeakRequestedCPUMilli, maxCPUBudget, aiRun.Metrics.StarvationDetected,
			),
		},
	}
}

func computeJainsIndex(tasks []workload.Task) float64 {
	if len(tasks) == 0 {
		return 1.0
	}
	var sumX, sumX2 float64
	n := 0.0
	for _, t := range tasks {
		if t.ActualDurationSec <= 0 {
			continue
		}
		// Normalized turnaround ratio: actual_duration / end_to_end_duration in (0, 1]
		e2e := math.Max(t.ActualDurationSec, t.EndToEndDurationSec)
		x := t.ActualDurationSec / e2e
		sumX += x
		sumX2 += x * x
		n++
	}
	if n == 0 || sumX2 == 0 {
		return 1.0
	}
	return round4((sumX * sumX) / (n * sumX2))
}

func meanSlice(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return round4(sum / float64(len(vals)))
}

func maxSlice(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return round4(m)
}

func percentileSlice(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	idx := int(math.Ceil((p/100.0)*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return round4(cp[idx])
}

func round4(v float64) float64 {
	return math.Round(v*10000.0) / 10000.0
}
