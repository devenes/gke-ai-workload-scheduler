package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/kubernetes"
	"github.com/devenes/gke-ai-workload-scheduler/internal/observability"
	"github.com/devenes/gke-ai-workload-scheduler/internal/policies"
	"github.com/devenes/gke-ai-workload-scheduler/internal/predictor"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/scheduler"
	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

//go:embed ui/*
var uiFS embed.FS

const maxRequestBodyBytes = 512 * 1024

// Server exposes the REST API, Prometheus /metrics, SSE stream, and embedded Web UI.
type Server struct {
	cfg           config.Config
	store         *queue.Store
	engine        *scheduler.Engine
	predictor     *predictor.Client
	kube          *kubernetes.Client
	recorder      *observability.Recorder
	mu            sync.Mutex
	activeLiveRun string
	subMu         sync.Mutex
	nextSubID     int64
	subscribers   map[int64]chan any
}

// NewServer constructs an API Server and wires real-time SSE broadcasts to the scheduler engine.
func NewServer(
	cfg config.Config,
	store *queue.Store,
	engine *scheduler.Engine,
	pred *predictor.Client,
	kube *kubernetes.Client,
	rec *observability.Recorder,
) *Server {
	s := &Server{
		cfg:         cfg,
		store:       store,
		engine:      engine,
		predictor:   pred,
		kube:        kube,
		recorder:    rec,
		subscribers: make(map[int64]chan any),
	}
	engine.SetUpdateHook(func(run queue.BenchmarkRun) {
		s.broadcast(map[string]any{
			"type": "run_update",
			"run":  run,
			"ts":   time.Now().UTC().Format(time.RFC3339),
		})
	})
	return s
}

// Handler returns the root http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleHealthz)
	mux.Handle("GET /metrics", s.recorder.Handler())

	mux.HandleFunc("GET /api/v1/overview", s.handleOverview)
	mux.HandleFunc("GET /api/v1/policies", s.handleListPolicies)
	mux.HandleFunc("POST /api/v1/policies/select", s.handleSelectPolicy)
	mux.HandleFunc("GET /api/v1/tasks", s.handleListTasks)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("POST /api/v1/tasks/submit", s.handleSubmitTasks)
	mux.HandleFunc("POST /api/v1/simulations/run", s.handleRunSimulation)
	mux.HandleFunc("POST /api/v1/benchmarks/live", s.handleRunLiveBenchmark)
	mux.HandleFunc("GET /api/v1/benchmarks", s.handleListBenchmarks)
	mux.HandleFunc("GET /api/v1/benchmarks/compare", s.handleCompareBenchmarks)
	mux.HandleFunc("GET /api/v1/benchmarks/{id}", s.handleGetBenchmark)
	mux.HandleFunc("GET /api/v1/predictor/metadata", s.handlePredictorMetadata)
	mux.HandleFunc("POST /api/v1/predictor/simulate-outage", s.handlePredictorOutage)
	mux.HandleFunc("GET /api/v1/stream", s.handleSSE)

	subUI, err := fs.Sub(uiFS, "ui")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(subUI))
	mux.Handle("/", fileServer)

	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	predHealth, predErr := s.predictor.Health(ctx)
	predStatus := "healthy"
	if predErr != nil {
		predStatus = "fallback_active"
	}

	activePol, _ := s.engine.GetActivePolicy()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":              "ok",
		"service":             "gke-ai-workload-scheduler",
		"project_id":          s.cfg.ProjectID,
		"cluster_name":        s.cfg.ClusterName,
		"namespace":           s.cfg.Namespace,
		"k8s_connected":       s.kube != nil && s.kube.IsConnected(),
		"active_policy":       activePol,
		"max_concurrent_jobs": s.cfg.MaxConcurrentJobs,
		"predictor_status":    predStatus,
		"predictor_health":    predHealth,
		"app_image_digest":    s.cfg.AppImageDigest,
		"timestamp":           time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	counts := s.store.StatusCounts()
	s.recorder.UpdateQueueGauges(counts)

	queueDepth := counts[workload.StatusPending] + counts[workload.StatusRanked] + counts[workload.StatusSubmitted]
	activePol, weights := s.engine.GetActivePolicy()

	predHealth, predErr := s.predictor.Health(ctx)
	predHealthy := predErr == nil
	predVer := "fallback-heuristic-v1"
	if predHealthy && predHealth != nil {
		if v, ok := predHealth["model_version"].(string); ok {
			predVer = v
		}
	}

	var k8sOv kubernetes.ClusterOverview
	if s.kube != nil {
		k8sOv = s.kube.Overview(ctx)
	}

	runs := s.store.ListRuns()
	recentRuns := runs
	if len(recentRuns) > 8 {
		recentRuns = recentRuns[:8]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"project_id":           s.cfg.ProjectID,
		"region":               s.cfg.Region,
		"zone":                 s.cfg.Zone,
		"cluster":              k8sOv,
		"queue_depth":          queueDepth,
		"status_counts":        counts,
		"active_policy":        activePol,
		"weights":              weights,
		"concurrency_limit":    s.cfg.MaxConcurrentJobs,
		"cpu_budget_milli":     s.cfg.MaxCPUBudgetMilli,
		"memory_budget_mb":     s.cfg.MaxMemoryBudgetMB,
		"predictor_healthy":    predHealthy,
		"predictor_version":    predVer,
		"predictor_health":     predHealth,
		"total_runs":           len(runs),
		"recent_runs":          recentRuns,
		"monitoring_dashboard": fmt.Sprintf("https://console.cloud.google.com/monitoring/dashboards?project=%s", s.cfg.ProjectID),
		"checked_at":           time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	activePol, weights := s.engine.GetActivePolicy()
	writeJSON(w, http.StatusOK, map[string]any{
		"active_policy": activePol,
		"weights":       weights,
		"policies":      policies.Catalog(),
	})
}

func (s *Server) handleSelectPolicy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Policy  string            `json:"policy"`
		Weights *policies.Weights `json:"weights,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.engine.SetActivePolicy(policies.PolicyName(body.Policy), body.Weights); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	activePol, weights := s.engine.GetActivePolicy()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "updated",
		"active_policy": activePol,
		"weights":       weights,
	})
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
	tasks := s.store.ListTasks(runID)
	limit := 150
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if n, err := strconv.Atoi(lStr); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(tasks),
		"tasks": tasks,
	})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	t, ok := s.store.GetTask(taskID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("task %q not found", taskID))
		return
	}

	var k8sObs *kubernetes.JobObservation
	var podLogs string
	if s.kube != nil && s.kube.IsConnected() && t.K8sJobName != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if obs, err := s.kube.InspectJobAndPod(ctx, t.K8sJobName); err == nil {
			k8sObs = &obs
			if r.URL.Query().Get("logs") == "true" && obs.PodName != "" {
				podLogs, _ = s.kube.GetPodLogs(ctx, obs.PodName)
			}
		}
	} else if r.URL.Query().Get("logs") == "true" {
		podLogs = fmt.Sprintf(
			`{"event":"simulated_execution","task_id":%q,"workload_class":%q,"predicted_duration_sec":%.4f,"actual_duration_sec":%.4f,"policy":%q,"rank":%d}`,
			t.TaskID, t.WorkloadClass, t.PredictedDurationSec, t.ActualDurationSec, t.PolicyUsed, t.DispatchRank,
		)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"task":           t,
		"k8s_job_status": k8sObs,
		"pod_logs":       podLogs,
	})
}

func (s *Server) handleSubmitTasks(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scenario       string            `json:"scenario"`
		Seed           int64             `json:"seed"`
		TaskCount      int               `json:"task_count"`
		Policy         string            `json:"policy"`
		IdempotencyKey string            `json:"idempotency_key"`
		Tasks          []workload.Task   `json:"tasks,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	pol, weights := s.engine.GetActivePolicy()
	if body.Policy != "" {
		if !policies.IsValidPolicy(body.Policy) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid policy %q", body.Policy))
			return
		}
		pol = policies.PolicyName(body.Policy)
	}

	var tasksToSubmit []workload.Task
	var fp string
	if len(body.Tasks) > 0 {
		if len(body.Tasks) > 200 {
			writeError(w, http.StatusBadRequest, "tasks slice cannot exceed 200 items")
			return
		}
		for i, t := range body.Tasks {
			if !workload.IsValidWorkloadClass(string(t.WorkloadClass)) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d] invalid workload_class %q", i, t.WorkloadClass))
				return
			}
			if t.CPURequestMilli < 50 || t.CPURequestMilli > 4000 {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d] cpu_request_milli out of bounds [50, 4000]", i))
				return
			}
			if t.MemoryRequestMB < 32 || t.MemoryRequestMB > 8192 {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d] memory_request_mb out of bounds [32, 8192]", i))
				return
			}
			if t.InputSize < 1 || t.InputSize > 2000 {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d] input_size out of bounds [1, 2000]", i))
				return
			}
			if t.TaskID == "" {
				t.TaskID = fmt.Sprintf("custom-task-%d-%03d", time.Now().UTC().UnixMilli(), i+1)
			}
			if t.ArrivalSequence <= 0 {
				t.ArrivalSequence = i + 1
			}
			if t.ComputeIntensity <= 0 {
				t.ComputeIntensity = 3.5
			}
			if t.MemoryWorkingSetMB <= 0 {
				t.MemoryWorkingSetMB = t.MemoryRequestMB / 2
			}
			if t.HistoricalMeanSec <= 0 {
				t.HistoricalMeanSec = workload.HistoricalClassSpecs[t.WorkloadClass].HistoricalMeanSec
			}
			if t.CalibratedTargetSec <= 0 {
				t.CalibratedTargetSec = workload.ComputeCalibratedDurationSec(
					t.WorkloadClass, t.InputSize, t.CPURequestMilli, t.MemoryRequestMB, t.ComputeIntensity, t.MemoryWorkingSetMB,
				)
			}
			tasksToSubmit = append(tasksToSubmit, t)
		}
	} else {
		if body.TaskCount <= 0 {
			body.TaskCount = 10
		}
		batch, err := workload.GenerateBatch(workload.BatchSpec{
			Scenario:  body.Scenario,
			Seed:      body.Seed,
			TaskCount: body.TaskCount,
		})
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		tasksToSubmit = batch.Tasks
		fp = batch.Fingerprint
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	preds := s.predictor.PredictBatch(ctx, tasksToSubmit)
	now := time.Now().UTC()

	for i := range tasksToSubmit {
		t := &tasksToSubmit[i]
		t.SubmittedAt = &now
		if t.HasDeadline && t.DeadlineAt == nil && t.DeadlineOffsetSec > 0 {
			dl := now.Add(time.Duration(t.DeadlineOffsetSec * float64(time.Second)))
			t.DeadlineAt = &dl
		}
		if i < len(preds) {
			t.PredictedDurationSec = preds[i].PredictedDurationSec
			t.EstimatedDurationSec = preds[i].PredictedDurationSec
			t.PredictorModelVer = preds[i].ModelVersion
			t.FallbackUsed = preds[i].FallbackUsed
			t.FallbackReason = preds[i].FallbackReason
		}
	}

	ranked := policies.RankTasks(pol, tasksToSubmit, now, weights)
	dedupCount := 0
	stored := make([]workload.Task, 0, len(ranked))
	for _, rt := range ranked {
		st, dedup, err := s.store.SubmitTask(rt)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if dedup {
			dedupCount++
		}
		stored = append(stored, st)
	}

	s.recorder.UpdateQueueGauges(s.store.StatusCounts())
	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "submitted",
		"policy":             pol,
		"batch_fingerprint":  fp,
		"submitted_count":    len(stored),
		"deduplicated_count": dedupCount,
		"tasks":              stored,
	})
}

func (s *Server) handleRunSimulation(w http.ResponseWriter, r *http.Request) {
	var body struct {
		scheduler.RunRequest
		AllPolicies bool `json:"all_policies"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.TaskCount == 0 {
		body.TaskCount = 20
	}
	if body.Concurrency == 0 {
		body.Concurrency = s.cfg.MaxConcurrentJobs
	}
	if body.Scenario == "" {
		body.Scenario = "mixed"
	}
	if body.Seed == 0 {
		body.Seed = 42
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	if body.AllPolicies {
		report, err := s.engine.CompareAllPoliciesSimulation(ctx, body.Scenario, body.Seed, body.TaskCount, body.Concurrency)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}

	run, err := s.engine.RunSimulation(ctx, body.RunRequest)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleRunLiveBenchmark(w http.ResponseWriter, r *http.Request) {
	var body struct {
		scheduler.RunRequest
		Async bool `json:"async"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body.Mode = queue.ModeLiveGKE
	if body.TaskCount == 0 {
		body.TaskCount = 10
	}
	if body.Concurrency == 0 {
		body.Concurrency = s.cfg.MaxConcurrentJobs
	}
	if err := s.engine.ValidateRunRequest(&body.RunRequest); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.kube == nil || !s.kube.IsConnected() {
		writeError(w, http.StatusServiceUnavailable, "Kubernetes cluster not connected (running in local mode)")
		return
	}

	// Check idempotency key first before checking single-flight lock
	if body.IdempotencyKey != "" {
		for _, existing := range s.store.ListRuns() {
			if existing.IdempotencyKey == body.IdempotencyKey {
				writeJSON(w, http.StatusOK, existing)
				return
			}
		}
	}

	s.mu.Lock()
	if s.activeLiveRun != "" {
		activeID := s.activeLiveRun
		s.mu.Unlock()
		writeError(w, http.StatusConflict, fmt.Sprintf("live GKE benchmark %q is currently running; wait for completion", activeID))
		return
	}
	if body.RunID == "" {
		body.RunID = fmt.Sprintf("gke-%s-%s-s%d-n%d-c%d-%d", body.Policy, body.Scenario, body.Seed, body.TaskCount, body.Concurrency, time.Now().UTC().Unix()%10000)
	}
	s.activeLiveRun = body.RunID
	s.mu.Unlock()

	if body.Async {
		go func(req scheduler.RunRequest) {
			defer func() {
				s.mu.Lock()
				s.activeLiveRun = ""
				s.mu.Unlock()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			_, _ = s.engine.RunLiveBenchmark(ctx, req)
		}(body.RunRequest)

		writeJSON(w, http.StatusAccepted, map[string]any{
			"status":  "started",
			"run_id":  body.RunID,
			"mode":    "live_gke",
			"policy":  body.Policy,
			"message": "Live GKE benchmark started asynchronously; monitor /api/v1/benchmarks/" + body.RunID + " or /api/v1/stream",
		})
		return
	}

	defer func() {
		s.mu.Lock()
		s.activeLiveRun = ""
		s.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	run, err := s.engine.RunLiveBenchmark(ctx, body.RunRequest)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleListBenchmarks(w http.ResponseWriter, r *http.Request) {
	runs := s.store.ListRuns()
	modeFilter := strings.TrimSpace(r.URL.Query().Get("mode"))
	if modeFilter != "" {
		filtered := make([]queue.BenchmarkRun, 0, len(runs))
		for _, run := range runs {
			if string(run.Mode) == modeFilter {
				filtered = append(filtered, run)
			}
		}
		runs = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(runs),
		"runs":  runs,
	})
}

func (s *Server) handleGetBenchmark(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, ok := s.store.GetRun(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("benchmark run %q not found", id))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleCompareBenchmarks(w http.ResponseWriter, r *http.Request) {
	scenario := r.URL.Query().Get("scenario")
	if scenario == "" {
		scenario = "deadline_sensitive"
	}
	seed := int64(42)
	if sStr := r.URL.Query().Get("seed"); sStr != "" {
		if n, err := strconv.ParseInt(sStr, 10, 64); err == nil {
			seed = n
		}
	}
	taskCount := 24
	if nStr := r.URL.Query().Get("task_count"); nStr != "" {
		if n, err := strconv.Atoi(nStr); err == nil && n >= 1 && n <= 200 {
			taskCount = n
		}
	}
	concurrency := s.cfg.MaxConcurrentJobs
	if cStr := r.URL.Query().Get("concurrency"); cStr != "" {
		if n, err := strconv.Atoi(cStr); err == nil && n >= 1 && n <= 12 {
			concurrency = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	report, err := s.engine.CompareAllPoliciesSimulation(ctx, scenario, seed, taskCount, concurrency)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handlePredictorMetadata(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	meta, err := s.predictor.Metadata(ctx)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":          "fallback_mode",
			"fallback_active": true,
			"reason":          err.Error(),
			"model_version":   "fallback-heuristic-v1",
		})
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

func (s *Server) handlePredictorOutage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ForceFail bool `json:"force_fail"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.predictor.SetForceFail(body.ForceFail)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "updated",
		"force_fail": s.predictor.IsForceFail(),
	})
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan any, 32)
	s.subMu.Lock()
	s.nextSubID++
	subID := s.nextSubID
	s.subscribers[subID] = ch
	s.subMu.Unlock()

	defer func() {
		s.subMu.Lock()
		delete(s.subscribers, subID)
		s.subMu.Unlock()
	}()

	// Send initial snapshot
	initEnv := map[string]any{
		"type":          "snapshot",
		"status_counts": s.store.StatusCounts(),
		"ts":            time.Now().UTC().Format(time.RFC3339),
	}
	if b, err := json.Marshal(initEnv); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", string(b))
		flusher.Flush()
	}

	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if b, err := json.Marshal(msg); err == nil {
				fmt.Fprintf(w, "data: %s\n\n", string(b))
				flusher.Flush()
			}
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) broadcast(msg any) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}

func readJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return fmt.Errorf("request body is empty")
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes))
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return fmt.Errorf("request body cannot be empty")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error":  msg,
		"status": status,
	})
}
