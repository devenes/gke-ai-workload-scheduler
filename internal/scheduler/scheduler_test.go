package scheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/dispatcher"
	"github.com/devenes/gke-ai-workload-scheduler/internal/observability"
	"github.com/devenes/gke-ai-workload-scheduler/internal/policies"
	"github.com/devenes/gke-ai-workload-scheduler/internal/predictor"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/scheduler"
	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

func TestWorkloadGeneratorDeterminism(t *testing.T) {
	b1, err := workload.GenerateBatch(workload.BatchSpec{Scenario: "mixed", Seed: 42, TaskCount: 25})
	if err != nil {
		t.Fatalf("GenerateBatch b1 failed: %v", err)
	}
	b2, err := workload.GenerateBatch(workload.BatchSpec{Scenario: "mixed", Seed: 42, TaskCount: 25})
	if err != nil {
		t.Fatalf("GenerateBatch b2 failed: %v", err)
	}
	b3, err := workload.GenerateBatch(workload.BatchSpec{Scenario: "mixed", Seed: 99, TaskCount: 25})
	if err != nil {
		t.Fatalf("GenerateBatch b3 failed: %v", err)
	}

	if b1.Fingerprint != b2.Fingerprint {
		t.Fatalf("expected identical fingerprint for identical seed, got %s vs %s", b1.Fingerprint, b2.Fingerprint)
	}
	if b1.Fingerprint == b3.Fingerprint {
		t.Fatalf("expected distinct fingerprint for different seeds")
	}
	if len(b1.Tasks) != 25 {
		t.Fatalf("expected 25 tasks, got %d", len(b1.Tasks))
	}
}

func TestSchedulingPoliciesBehavior(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	subEarly := now.Add(-40 * time.Second)
	subRecent := now.Add(-1 * time.Second)
	dlSoon := now.Add(4 * time.Second)
	dlLater := now.Add(20 * time.Second)
	dlExpired := now.Add(-5 * time.Second)

	tasks := []workload.Task{
		{
			TaskID:               "t-long-early",
			WorkloadClass:        workload.ClassLongCPU,
			ArrivalSequence:      1,
			Priority:             1, // Low base priority, but waited 40s!
			CPURequestMilli:      250,
			MemoryRequestMB:      128,
			PredictedDurationSec: 9.5,
			SubmittedAt:          &subEarly,
			HasDeadline:          true,
			DeadlineAt:           &dlLater,
		},
		{
			TaskID:               "t-short-urgent",
			WorkloadClass:        workload.ClassShortCPU,
			ArrivalSequence:      2,
			Priority:             8,
			CPURequestMilli:      200,
			MemoryRequestMB:      128,
			PredictedDurationSec: 1.8,
			SubmittedAt:          &subRecent,
			HasDeadline:          true,
			DeadlineAt:           &dlSoon,
		},
		{
			TaskID:               "t-expired-deadline",
			WorkloadClass:        workload.ClassDeadlineSensitive,
			ArrivalSequence:      3,
			Priority:             5,
			CPURequestMilli:      200,
			MemoryRequestMB:      128,
			PredictedDurationSec: 4.0,
			SubmittedAt:          &subRecent,
			HasDeadline:          true,
			DeadlineAt:           &dlExpired,
		},
		{
			TaskID:               "t-no-deadline",
			WorkloadClass:        workload.ClassMixed,
			ArrivalSequence:      4,
			Priority:             9,
			CPURequestMilli:      200,
			MemoryRequestMB:      128,
			PredictedDurationSec: 5.0,
			SubmittedAt:          &subRecent,
			HasDeadline:          false,
		},
	}

	w := policies.DefaultWeights()

	// 1. FIFO
	fifo := policies.RankTasks(policies.PolicyFIFO, tasks, now, w)
	if fifo[0].TaskID != "t-long-early" || fifo[1].TaskID != "t-short-urgent" || fifo[2].TaskID != "t-expired-deadline" || fifo[3].TaskID != "t-no-deadline" {
		t.Fatalf("unexpected FIFO order: %+v", []string{fifo[0].TaskID, fifo[1].TaskID, fifo[2].TaskID, fifo[3].TaskID})
	}

	// 2. SJF (Prediction-based)
	sjf := policies.RankTasks(policies.PolicySJF, tasks, now, w)
	if sjf[0].TaskID != "t-short-urgent" || sjf[3].TaskID != "t-long-early" {
		t.Fatalf("unexpected SJF order: first=%s last=%s", sjf[0].TaskID, sjf[3].TaskID)
	}

	// 3. EDF (Tier 1 viable deadlines first by earliest deadline, then Tier 2 expired, then Tier 3 no deadline)
	edf := policies.RankTasks(policies.PolicyEDF, tasks, now, w)
	if edf[0].TaskID != "t-short-urgent" || edf[1].TaskID != "t-long-early" || edf[2].TaskID != "t-expired-deadline" || edf[3].TaskID != "t-no-deadline" {
		t.Fatalf("unexpected EDF order: %+v", []string{edf[0].TaskID, edf[1].TaskID, edf[2].TaskID, edf[3].TaskID})
	}

	// 4. Priority with Aging: t-long-early (base=1 + 1.5*(40/5)=13.0) overtakes t-no-deadline (base=9 + 0.3=9.3)
	pa := policies.RankTasks(policies.PolicyPriorityAging, tasks, now, w)
	if pa[0].TaskID != "t-long-early" {
		t.Fatalf("expected aging to boost t-long-early to #1, got %s (eff_prio=%.2f)", pa[0].TaskID, pa[0].ScoreBreakdown.EffectivePriority)
	}

	// 5. AI-Assisted: t-short-urgent has short predicted runtime (1.8s), tight viable deadline slack (2.2s), and high priority (8)
	ai := policies.RankTasks(policies.PolicyAIAssisted, tasks, now, w)
	if ai[0].TaskID != "t-short-urgent" {
		t.Fatalf("expected AI-Assisted to rank t-short-urgent #1, got %s", ai[0].TaskID)
	}
}

func TestStoreIdempotencyAndOptimisticConcurrency(t *testing.T) {
	store, err := queue.NewStore(t.TempDir(), t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	task := workload.Task{
		TaskID:          "task-idem-1",
		IdempotencyKey:  "idem-key-1",
		WorkloadClass:   workload.ClassShortCPU,
		ArrivalSequence: 1,
		CPURequestMilli: 200,
		MemoryRequestMB: 128,
	}

	sub1, dedup1, err := store.SubmitTask(task)
	if err != nil || dedup1 {
		t.Fatalf("first submit failed: err=%v dedup=%v", err, dedup1)
	}
	// Duplicate submit with different TaskID but same IdempotencyKey must deduplicate
	dupTask := task
	dupTask.TaskID = "task-idem-duplicate"
	sub2, dedup2, err := store.SubmitTask(dupTask)
	if err != nil || !dedup2 || sub2.TaskID != sub1.TaskID {
		t.Fatalf("expected idempotent deduplication, got dedup=%v task_id=%s", dedup2, sub2.TaskID)
	}

	// Conditional update with correct ResourceVersion succeeds
	u1, err := store.UpdateTaskConditional(sub1.TaskID, sub1.ResourceVersion, func(tk *workload.Task) error {
		tk.Status = workload.StatusRunning
		return nil
	})
	if err != nil || u1.ResourceVersion != 2 {
		t.Fatalf("expected ResourceVersion=2, got %d err=%v", u1.ResourceVersion, err)
	}

	// Conditional update with stale ResourceVersion=1 must return ErrConflict
	_, err = store.UpdateTaskConditional(sub1.TaskID, 1, func(tk *workload.Task) error {
		tk.Status = workload.StatusSucceeded
		return nil
	})
	if !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("expected ErrConflict on stale ResourceVersion, got %v", err)
	}
}

func TestAdmissionGateConcurrencyAndResourceBudgets(t *testing.T) {
	gate := dispatcher.NewGate(dispatcher.BudgetConfig{
		MaxConcurrentJobs: 2,
		MaxCPUBudgetMilli: 500,
		MaxMemoryBudgetMB: 512,
	})

	t1 := workload.Task{TaskID: "t1", CPURequestMilli: 250, MemoryRequestMB: 256}
	t2 := workload.Task{TaskID: "t2", CPURequestMilli: 300, MemoryRequestMB: 128} // Exceeds 500m CPU when t1 active!
	t3 := workload.Task{TaskID: "t3", CPURequestMilli: 250, MemoryRequestMB: 256} // Fits alongside t1!
	t4 := workload.Task{TaskID: "t4", CPURequestMilli: 100, MemoryRequestMB: 64}  // Blocked by MaxConcurrentJobs=2!

	ok1, seq1, _ := gate.TryAdmit(t1)
	if !ok1 || seq1 != 1 {
		t.Fatalf("expected t1 admitted as #1")
	}
	ok2, _, reason2 := gate.TryAdmit(t2)
	if ok2 {
		t.Fatalf("expected t2 rejected due to CPU budget (250+300 > 500), got admitted")
	}
	if reason2 == "" {
		t.Fatalf("expected rejection reason")
	}

	ok3, seq3, _ := gate.TryAdmit(t3)
	if !ok3 || seq3 != 2 {
		t.Fatalf("expected t3 admitted as #2")
	}

	ok4, _, _ := gate.TryAdmit(t4)
	if ok4 {
		t.Fatalf("expected t4 rejected due to MaxConcurrentJobs=2")
	}

	gate.Release("t1")
	gate.Release("t3")
	ok2After, seq2After, _ := gate.TryAdmit(t2)
	if !ok2After || seq2After != 3 {
		t.Fatalf("expected t2 admitted as #3 after release")
	}
}

func TestSimulationAndPredictorFallback(t *testing.T) {
	// Mock predictor HTTP server returning accurate calibrated predictions
	mockPred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "model_version": "v1.0.0-cpu-gbr"})
			return
		}
		if r.URL.Path == "/predict" {
			var body struct {
				Tasks []predictor.TaskFeatureRequest `json:"tasks"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			preds := make([]predictor.PredictionResult, len(body.Tasks))
			for i, item := range body.Tasks {
				dur := workload.ComputeCalibratedDurationSec(
					workload.WorkloadClass(item.WorkloadClass),
					item.InputSize,
					item.CPURequestMilli,
					item.MemoryRequestMB,
					item.ComputeIntensity,
					item.MemoryWorkingSet,
				)
				preds[i] = predictor.PredictionResult{
					TaskID:               item.TaskID,
					PredictedDurationSec: dur,
					ModelVersion:         "v1.0.0-cpu-gbr",
					FallbackUsed:         false,
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"model_version": "v1.0.0-cpu-gbr",
				"count":         len(preds),
				"predictions":   preds,
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockPred.Close()

	cfg := config.Default()
	cfg.LocalMode = true
	cfg.StateDir = t.TempDir()
	cfg.ArtifactsDir = t.TempDir()
	cfg.PredictorURL = mockPred.URL

	store, err := queue.NewStore(cfg.StateDir, cfg.ArtifactsDir, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	predClient := predictor.NewClient(cfg.PredictorURL, 2*time.Second)
	rec := observability.NewRecorder()
	engine := scheduler.NewEngine(cfg, store, predClient, nil, rec)

	ctx := context.Background()
	cmp, err := engine.CompareAllPoliciesSimulation(ctx, "deadline_sensitive", 42, 24, 4)
	if err != nil {
		t.Fatalf("CompareAllPoliciesSimulation failed: %v", err)
	}
	if len(cmp.RunsByPolicy) != 5 {
		t.Fatalf("expected 5 policy runs, got %d", len(cmp.RunsByPolicy))
	}

	aiRun := cmp.RunsByPolicy["ai_assisted"]
	fifoRun := cmp.RunsByPolicy["fifo"]
	if aiRun.Metrics.MeanQueueWaitSec >= fifoRun.Metrics.MeanQueueWaitSec {
		t.Fatalf("expected AI-Assisted mean queue wait (%.3fs) < FIFO mean queue wait (%.3fs)",
			aiRun.Metrics.MeanQueueWaitSec, fifoRun.Metrics.MeanQueueWaitSec)
	}

	// Test predictor fallback when outage is forced
	predClient.SetForceFail(true)
	fbRun, err := engine.RunSimulation(ctx, scheduler.RunRequest{
		Policy:      policies.PolicyAIAssisted,
		Scenario:    "mixed",
		Seed:        42,
		TaskCount:   10,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("RunSimulation with fallback failed: %v", err)
	}
	if fbRun.Metrics.PredictorFallbackCount != 10 {
		t.Fatalf("expected 10 fallback predictions recorded, got %d", fbRun.Metrics.PredictorFallbackCount)
	}
}
