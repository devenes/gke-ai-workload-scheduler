package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

// ErrConflict is returned when a conditional update provides a stale ResourceVersion.
var ErrConflict = errors.New("optimistic concurrency conflict: resource_version mismatch")

// ErrNotFound is returned when a requested task or benchmark run does not exist.
var ErrNotFound = errors.New("requested resource not found")

// ErrQueueFull is returned when the queue capacity limit is reached.
var ErrQueueFull = errors.New("queue capacity exceeded")

const MaxQueueCapacity = 1000

// RunMode distinguishes offline simulation from live GKE execution.
type RunMode string

const (
	ModeSimulation RunMode = "simulation"
	ModeLiveGKE    RunMode = "live_gke"
)

// RunSummaryMetrics holds comprehensive benchmark evaluation metrics required by Section 10.3.
type RunSummaryMetrics struct {
	SubmittedTasks              int     `json:"submitted_tasks"`
	SucceededTasks              int     `json:"succeeded_tasks"`
	FailedTasks                 int     `json:"failed_tasks"`
	TimedOutTasks               int     `json:"timed_out_tasks"`
	CancelledTasks              int     `json:"cancelled_tasks"`
	SampleSize                  int     `json:"sample_size"`
	PercentilesMeaningful       bool    `json:"percentiles_meaningful"`
	SampleLimitationNote        string  `json:"sample_limitation_note,omitempty"`
	MeanQueueWaitSec            float64 `json:"mean_queue_wait_sec"`
	P50QueueWaitSec             float64 `json:"p50_queue_wait_sec"`
	P95QueueWaitSec             float64 `json:"p95_queue_wait_sec"`
	P99QueueWaitSec             float64 `json:"p99_queue_wait_sec"`
	MaxQueueWaitSec             float64 `json:"max_queue_wait_sec"`
	MeanAdmissionDelaySec       float64 `json:"mean_admission_delay_sec"`
	MeanPodScheduleDelaySec     float64 `json:"mean_pod_schedule_delay_sec"`
	MeanExecutionDurationSec    float64 `json:"mean_execution_duration_sec"`
	P50ExecutionDurationSec     float64 `json:"p50_execution_duration_sec"`
	P95ExecutionDurationSec     float64 `json:"p95_execution_duration_sec"`
	MeanEndToEndSec             float64 `json:"mean_end_to_end_sec"`
	P50EndToEndSec              float64 `json:"p50_end_to_end_sec"`
	P95EndToEndSec              float64 `json:"p95_end_to_end_sec"`
	P99EndToEndSec              float64 `json:"p99_end_to_end_sec"`
	MakespanSec                 float64 `json:"makespan_sec"`
	ThroughputTasksPerMin       float64 `json:"throughput_tasks_per_min"`
	TasksWithDeadlines          int     `json:"tasks_with_deadlines"`
	DeadlineMetCount            int     `json:"deadline_met_count"`
	DeadlineMissCount           int     `json:"deadline_miss_count"`
	DeadlineSuccessRatePct      float64 `json:"deadline_success_rate_pct"`
	JainsFairnessIndex          float64 `json:"jains_fairness_index"`
	LowPriorityMaxWaitSec       float64 `json:"low_priority_max_wait_sec"`
	StarvationDetected          bool    `json:"starvation_detected"`
	PredictionMAESec            float64 `json:"prediction_mae_sec"`
	PredictionRMSESec           float64 `json:"prediction_rmse_sec"`
	BaselineClassMedianMAESec   float64 `json:"baseline_class_median_mae_sec"`
	PeakActiveJobs              int     `json:"peak_active_jobs"`
	PeakRequestedCPUMilli       int     `json:"peak_requested_cpu_milli"`
	PeakRequestedMemoryMB       int     `json:"peak_requested_memory_mb"`
	MeanObservedCPUMilli        int     `json:"mean_observed_cpu_milli"`
	MeanObservedMemoryMB        int     `json:"mean_observed_memory_mb"`
	PolicyDecisionCount         int     `json:"policy_decision_count"`
	PredictorFallbackCount      int     `json:"predictor_fallback_count"`
	DispatchOrderMatchesAdmit   bool    `json:"dispatch_order_matches_admission"`
	AdmissionOrderMatchesPodRun bool    `json:"admission_order_matches_pod_start"`
}

// BenchmarkRun represents a single offline simulation or live GKE benchmark execution.
type BenchmarkRun struct {
	RunID             string            `json:"run_id"`
	IdempotencyKey    string            `json:"idempotency_key,omitempty"`
	Mode              RunMode           `json:"mode"`
	Policy            string            `json:"policy"`
	Scenario          string            `json:"scenario"`
	Seed              int64             `json:"seed"`
	TaskCount         int               `json:"task_count"`
	Concurrency       int               `json:"concurrency"`
	CPUBudgetMilli    int               `json:"cpu_budget_milli"`
	MemoryBudgetMB    int               `json:"memory_budget_mb"`
	BatchFingerprint  string            `json:"batch_fingerprint_sha256"`
	Status            string            `json:"status"` // Running, Completed, Failed, Cancelled
	ResourceVersion   int64             `json:"resource_version"`
	PredictorModelVer string            `json:"predictor_model_version"`
	StartedAt         time.Time         `json:"started_at"`
	FinishedAt        *time.Time        `json:"finished_at,omitempty"`
	Metrics           RunSummaryMetrics `json:"metrics"`
	Tasks             []workload.Task   `json:"tasks"`
	ArtifactPath      string            `json:"artifact_path,omitempty"`
}

// KubeStateSyncer allows mirroring state to Kubernetes ConfigMaps/CRDs so state survives Pod restarts.
type KubeStateSyncer interface {
	SaveLedgerSnapshot(ctx context.Context, payload []byte) error
	LoadLedgerSnapshot(ctx context.Context) ([]byte, error)
	SyncBenchmarkRunCRD(ctx context.Context, run BenchmarkRun) error
}

type ledgerDiskFormat struct {
	UpdatedAt time.Time               `json:"updated_at"`
	Runs      map[string]BenchmarkRun `json:"runs"`
	Tasks     map[string]workload.Task `json:"tasks"`
}

// Store manages durable task queue state and benchmark runs with optimistic concurrency control.
type Store struct {
	mu           sync.RWMutex
	stateDir     string
	artifactsDir string
	ledgerPath   string
	runs         map[string]BenchmarkRun
	tasks        map[string]workload.Task
	idemRuns     map[string]string
	idemTasks    map[string]string
	kubeSyncer   KubeStateSyncer
}

// NewStore creates or recovers a durable Store from disk (and optional Kubernetes backing store).
func NewStore(stateDir, artifactsDir string, kubeSyncer KubeStateSyncer) (*Store, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(artifactsDir, "benchmarks"), 0o755); err != nil {
		return nil, fmt.Errorf("create benchmarks dir: %w", err)
	}
	s := &Store{
		stateDir:     stateDir,
		artifactsDir: artifactsDir,
		ledgerPath:   filepath.Join(stateDir, "ledger.json"),
		runs:         make(map[string]BenchmarkRun),
		tasks:        make(map[string]workload.Task),
		idemRuns:     make(map[string]string),
		idemTasks:    make(map[string]string),
		kubeSyncer:   kubeSyncer,
	}
	_ = s.load()
	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.ledgerPath)
	if err != nil && s.kubeSyncer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if kRaw, kErr := s.kubeSyncer.LoadLedgerSnapshot(ctx); kErr == nil && len(kRaw) > 0 {
			raw = kRaw
			err = nil
		}
	}
	if err != nil {
		return err
	}

	var disk ledgerDiskFormat
	if err := json.Unmarshal(raw, &disk); err != nil {
		return err
	}
	if disk.Runs != nil {
		s.runs = disk.Runs
		for id, r := range s.runs {
			if r.IdempotencyKey != "" {
				s.idemRuns[r.IdempotencyKey] = id
			}
		}
	}
	if disk.Tasks != nil {
		s.tasks = disk.Tasks
		for id, t := range s.tasks {
			if t.IdempotencyKey != "" {
				s.idemTasks[t.IdempotencyKey] = id
			}
		}
	}
	return nil
}

func (s *Store) persistLocked() error {
	disk := ledgerDiskFormat{
		UpdatedAt: time.Now().UTC(),
		Runs:      s.runs,
		Tasks:     s.tasks,
	}
	raw, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := s.ledgerPath + ".tmp"
	if err := os.WriteFile(tmpPath, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.ledgerPath); err != nil {
		return err
	}
	if s.kubeSyncer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.kubeSyncer.SaveLedgerSnapshot(ctx, raw)
	}
	return nil
}

// SubmitTask inserts a task idempotently into the queue. If a task with the same TaskID or
// IdempotencyKey already exists, it returns the existing task and deduplicated=true.
func (s *Store) SubmitTask(t workload.Task) (workload.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.tasks[t.TaskID]; ok {
		return existing, true, nil
	}
	if t.IdempotencyKey != "" {
		if existingID, ok := s.idemTasks[t.IdempotencyKey]; ok {
			if existing, exists := s.tasks[existingID]; exists {
				return existing, true, nil
			}
		}
	}

	activeQueueCount := 0
	for _, item := range s.tasks {
		if item.Status == workload.StatusPending || item.Status == workload.StatusRanked || item.Status == workload.StatusSubmitted {
			activeQueueCount++
		}
	}
	if activeQueueCount >= MaxQueueCapacity {
		return workload.Task{}, false, ErrQueueFull
	}

	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.SubmittedAt == nil {
		t.SubmittedAt = &now
	}
	if t.HasDeadline && t.DeadlineAt == nil && t.DeadlineOffsetSec > 0 {
		dl := now.Add(time.Duration(t.DeadlineOffsetSec * float64(time.Second)))
		t.DeadlineAt = &dl
	}
	if t.Status == "" {
		t.Status = workload.StatusPending
	}
	t.ResourceVersion = 1

	s.tasks[t.TaskID] = t
	if t.IdempotencyKey != "" {
		s.idemTasks[t.IdempotencyKey] = t.TaskID
	}
	_ = s.persistLocked()
	return t, false, nil
}

// UpdateTaskConditional updates a task only if its current ResourceVersion matches expectedVersion.
// On success, it increments ResourceVersion and persists the updated state.
func (s *Store) UpdateTaskConditional(taskID string, expectedVersion int64, mutate func(*workload.Task) error) (workload.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.tasks[taskID]
	if !ok {
		return workload.Task{}, ErrNotFound
	}
	if cur.ResourceVersion != expectedVersion {
		return workload.Task{}, fmt.Errorf("%w: have=%d want=%d for task %s", ErrConflict, cur.ResourceVersion, expectedVersion, taskID)
	}
	if err := mutate(&cur); err != nil {
		return workload.Task{}, err
	}
	cur.ResourceVersion++
	s.tasks[taskID] = cur
	_ = s.persistLocked()
	return cur, nil
}

// UpdateTaskWithRetry performs a concurrency-safe conditional update with automatic conflict retries.
func (s *Store) UpdateTaskWithRetry(taskID string, maxRetries int, mutate func(*workload.Task) error) (workload.Task, error) {
	if maxRetries < 1 {
		maxRetries = 5
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		cur, ok := s.GetTask(taskID)
		if !ok {
			return workload.Task{}, ErrNotFound
		}
		updated, err := s.UpdateTaskConditional(taskID, cur.ResourceVersion, mutate)
		if err == nil {
			return updated, nil
		}
		if !errors.Is(err, ErrConflict) {
			return workload.Task{}, err
		}
	}
	return workload.Task{}, ErrConflict
}

// GetTask returns a task by ID.
func (s *Store) GetTask(taskID string) (workload.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[taskID]
	return t, ok
}

// ListTasks returns all tasks sorted by RunID, DispatchRank, and ArrivalSequence.
func (s *Store) ListTasks(runID string) []workload.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]workload.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		if runID != "" && t.RunID != runID {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RunID != out[j].RunID {
			return out[i].RunID > out[j].RunID
		}
		if out[i].DispatchRank != out[j].DispatchRank && out[i].DispatchRank > 0 && out[j].DispatchRank > 0 {
			return out[i].DispatchRank < out[j].DispatchRank
		}
		return out[i].ArrivalSequence < out[j].ArrivalSequence
	})
	return out
}

// SaveRun creates or updates a BenchmarkRun and writes its individual JSON artifact.
func (s *Store) SaveRun(run BenchmarkRun) (BenchmarkRun, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if run.IdempotencyKey != "" {
		if existingID, ok := s.idemRuns[run.IdempotencyKey]; ok && existingID != run.RunID {
			if existing, exists := s.runs[existingID]; exists {
				return existing, true, nil
			}
		}
	}

	run.ResourceVersion++
	artifactPath := filepath.Join(s.artifactsDir, "benchmarks", fmt.Sprintf("%s.json", run.RunID))
	run.ArtifactPath = artifactPath

	s.runs[run.RunID] = run
	if run.IdempotencyKey != "" {
		s.idemRuns[run.IdempotencyKey] = run.RunID
	}
	for _, t := range run.Tasks {
		if t.ResourceVersion == 0 {
			t.ResourceVersion = 1
		}
		s.tasks[t.TaskID] = t
		if t.IdempotencyKey != "" {
			s.idemTasks[t.IdempotencyKey] = t.TaskID
		}
	}

	if rawRun, err := json.MarshalIndent(run, "", "  "); err == nil {
		_ = os.WriteFile(artifactPath, rawRun, 0o644)
	}
	_ = s.persistLocked()

	if s.kubeSyncer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.kubeSyncer.SyncBenchmarkRunCRD(ctx, run)
	}
	return run, false, nil
}

// GetRun returns a BenchmarkRun by ID.
func (s *Store) GetRun(runID string) (BenchmarkRun, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[runID]
	return r, ok
}

// ListRuns returns all BenchmarkRuns ordered newest first.
func (s *Store) ListRuns() []BenchmarkRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BenchmarkRun, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.After(out[j].StartedAt)
	})
	return out
}

// StatusCounts returns current counts of tasks by status.
func (s *Store) StatusCounts() map[workload.TaskStatus]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[workload.TaskStatus]int{
		workload.StatusPending:   0,
		workload.StatusRanked:    0,
		workload.StatusSubmitted: 0,
		workload.StatusAdmitted:  0,
		workload.StatusRunning:   0,
		workload.StatusSucceeded: 0,
		workload.StatusFailed:    0,
		workload.StatusTimedOut:  0,
		workload.StatusCancelled: 0,
	}
	for _, t := range s.tasks {
		counts[t.Status]++
	}
	return counts
}
