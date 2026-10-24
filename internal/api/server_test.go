package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/api"
	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/observability"
	"github.com/devenes/gke-ai-workload-scheduler/internal/predictor"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/scheduler"
)

func TestAPIEndpointsAndValidation(t *testing.T) {
	cfg := config.Default()
	cfg.LocalMode = true
	cfg.StateDir = t.TempDir()
	cfg.ArtifactsDir = t.TempDir()

	store, err := queue.NewStore(cfg.StateDir, cfg.ArtifactsDir, nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	predClient := predictor.NewClient("http://127.0.0.1:19999", 200*time.Millisecond)
	predClient.SetForceFail(true) // Deterministic fallback in unit test without external process
	rec := observability.NewRecorder()
	engine := scheduler.NewEngine(cfg, store, predClient, nil, rec)
	srv := api.NewServer(cfg, store, engine, predClient, nil, rec)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. GET /healthz
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz failed: err=%v status=%v", err, resp.StatusCode)
	}
	resp.Body.Close()

	// 2. GET / (UI HTML)
	uiResp, err := http.Get(ts.URL + "/")
	if err != nil || uiResp.StatusCode != http.StatusOK {
		t.Fatalf("GET / failed: %v", err)
	}
	uiBytes, _ := io.ReadAll(uiResp.Body)
	uiResp.Body.Close()
	if !strings.Contains(string(uiBytes), "GKE AI Workload Scheduler") {
		t.Fatalf("expected UI HTML to contain title")
	}

	// 3. POST /api/v1/policies/select (invalid policy -> 400)
	badPolBody, _ := json.Marshal(map[string]any{"policy": "nonexistent_policy"})
	badResp, err := http.Post(ts.URL+"/api/v1/policies/select", "application/json", bytes.NewReader(badPolBody))
	if err != nil || badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid policy, got %v", badResp.StatusCode)
	}
	badResp.Body.Close()

	// 4. POST /api/v1/policies/select (valid policy -> 200)
	okPolBody, _ := json.Marshal(map[string]any{"policy": "priority_aging"})
	okResp, err := http.Post(ts.URL+"/api/v1/policies/select", "application/json", bytes.NewReader(okPolBody))
	if err != nil || okResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for valid policy select, got %v", okResp.StatusCode)
	}
	okResp.Body.Close()

	// 5. POST /api/v1/tasks/submit
	subBody, _ := json.Marshal(map[string]any{
		"scenario":   "mixed",
		"seed":       42,
		"task_count": 6,
		"policy":     "ai_assisted",
	})
	subResp, err := http.Post(ts.URL+"/api/v1/tasks/submit", "application/json", bytes.NewReader(subBody))
	if err != nil || subResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for tasks/submit, got %v", subResp.StatusCode)
	}
	subResp.Body.Close()

	// 6. POST /api/v1/simulations/run
	simBody, _ := json.Marshal(map[string]any{
		"policy":      "ai_assisted",
		"scenario":    "deadline_sensitive",
		"seed":        42,
		"task_count":  12,
		"concurrency": 4,
	})
	simResp, err := http.Post(ts.URL+"/api/v1/simulations/run", "application/json", bytes.NewReader(simBody))
	if err != nil || simResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for simulations/run, got %v", simResp.StatusCode)
	}
	simResp.Body.Close()

	// 7. GET /metrics
	mResp, err := http.Get(ts.URL + "/metrics")
	if err != nil || mResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for /metrics, got %v", mResp.StatusCode)
	}
	mBytes, _ := io.ReadAll(mResp.Body)
	mResp.Body.Close()
	if !strings.Contains(string(mBytes), "scheduler_tasks_submitted_total") {
		t.Fatalf("expected Prometheus metrics in /metrics response")
	}
}
