package predictor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

// TaskFeatureRequest is the strictly pre-execution payload sent to the Python runtime predictor.
type TaskFeatureRequest struct {
	TaskID            string  `json:"task_id,omitempty"`
	WorkloadClass     string  `json:"workload_class"`
	InputSize         float64 `json:"input_size"`
	CPURequestMilli   int     `json:"cpu_request_milli"`
	MemoryRequestMB   int     `json:"memory_request_mb"`
	ComputeIntensity  float64 `json:"compute_intensity"`
	MemoryWorkingSet  int     `json:"memory_working_set_mb"`
	HistoricalMeanSec float64 `json:"historical_class_mean_sec"`
}

// PredictionResult represents the predictor output or explicit fallback output for a task.
type PredictionResult struct {
	TaskID               string  `json:"task_id"`
	PredictedDurationSec float64 `json:"predicted_duration_sec"`
	UncertaintyMAESec    float64 `json:"uncertainty_mae_sec"`
	ModelVersion         string  `json:"model_version"`
	FallbackUsed         bool    `json:"fallback_used"`
	FallbackReason       string  `json:"fallback_reason,omitempty"`
	BaselineClassMedian  float64 `json:"baseline_class_median_sec"`
	PredictionLatencyMs  float64 `json:"prediction_latency_ms"`
	PredictedAt          string  `json:"predicted_at"`
}

// Client communicates with the Python scikit-learn runtime predictor service.
type Client struct {
	baseURL    string
	httpClient *http.Client
	mu         sync.RWMutex
	forceFail  bool
}

// NewClient constructs a predictor Client with bounded HTTP timeouts.
func NewClient(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 2500 * time.Millisecond
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// SetForceFail enables or disables simulated predictor failure for fallback testing (T07).
func (c *Client) SetForceFail(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forceFail = fail
}

// IsForceFail returns whether simulated predictor failure is enabled.
func (c *Client) IsForceFail() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.forceFail
}

// Health queries GET /healthz on the predictor service.
func (c *Client) Health(ctx context.Context) (map[string]any, error) {
	if c.IsForceFail() {
		return nil, fmt.Errorf("predictor client forced failure mode active")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("predictor healthz status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Metadata queries GET /metadata on the predictor service.
func (c *Client) Metadata(ctx context.Context) (map[string]any, error) {
	if c.IsForceFail() {
		return nil, fmt.Errorf("predictor client forced failure mode active")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/metadata", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("predictor metadata status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// PredictBatch requests runtime predictions for a slice of tasks, falling back deterministically
// per task or batch if the predictor service is unreachable or returns an error.
func (c *Client) PredictBatch(ctx context.Context, tasks []workload.Task) []PredictionResult {
	if len(tasks) == 0 {
		return nil
	}
	t0 := time.Now()
	if c.IsForceFail() {
		return FallbackBatch(tasks, "forced_predictor_outage", time.Since(t0))
	}

	reqItems := make([]TaskFeatureRequest, len(tasks))
	for i, t := range tasks {
		reqItems[i] = TaskFeatureRequest{
			TaskID:            t.TaskID,
			WorkloadClass:     string(t.WorkloadClass),
			InputSize:         t.InputSize,
			CPURequestMilli:   t.CPURequestMilli,
			MemoryRequestMB:   t.MemoryRequestMB,
			ComputeIntensity:  t.ComputeIntensity,
			MemoryWorkingSet:  t.MemoryWorkingSetMB,
			HistoricalMeanSec: t.HistoricalMeanSec,
		}
	}

	payload, err := json.Marshal(map[string]any{"tasks": reqItems})
	if err != nil {
		return FallbackBatch(tasks, "marshal_error", time.Since(t0))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/predict", bytes.NewReader(payload))
	if err != nil {
		return FallbackBatch(tasks, "request_build_error", time.Since(t0))
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return FallbackBatch(tasks, fmt.Sprintf("predictor_unreachable: %v", err), time.Since(t0))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return FallbackBatch(tasks, "read_body_error", time.Since(t0))
	}

	if resp.StatusCode != http.StatusOK {
		return FallbackBatch(tasks, fmt.Sprintf("predictor_http_%d", resp.StatusCode), time.Since(t0))
	}

	var parsed struct {
		ModelVersion string             `json:"model_version"`
		Count        int                `json:"count"`
		Predictions  []PredictionResult `json:"predictions"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || len(parsed.Predictions) != len(tasks) {
		return FallbackBatch(tasks, "invalid_predictor_response_schema", time.Since(t0))
	}

	results := make([]PredictionResult, len(tasks))
	for i, p := range parsed.Predictions {
		if math.IsNaN(p.PredictedDurationSec) || math.IsInf(p.PredictedDurationSec, 0) || p.PredictedDurationSec <= 0 || p.PredictedDurationSec > 300 {
			results[i] = FallbackOne(tasks[i], "out_of_range_prediction_value", time.Since(t0))
			continue
		}
		p.TaskID = tasks[i].TaskID
		results[i] = p
	}
	return results
}

// FallbackOne computes a deterministic heuristic runtime estimate when the ML predictor is unavailable.
// It uses only historical class mean and requested resources, explicitly marking FallbackUsed=true.
func FallbackOne(t workload.Task, reason string, elapsed time.Duration) PredictionResult {
	spec, ok := workload.HistoricalClassSpecs[t.WorkloadClass]
	if !ok {
		spec = workload.HistoricalClassSpecs[workload.ClassMixed]
	}
	// Simple linear class-mean + input-size scaling heuristic (less accurate than GBR model, but safe & deterministic)
	sizeFactor := 0.65 + 0.35*(t.InputSize/100.0)
	cpuFactor := 250.0 / float64(max(50, t.CPURequestMilli))
	est := math.Max(0.8, math.Min(30.0, spec.HistoricalMeanSec*sizeFactor*math.Sqrt(cpuFactor)))
	est = math.Round(est*10000.0) / 10000.0

	return PredictionResult{
		TaskID:               t.TaskID,
		PredictedDurationSec: est,
		UncertaintyMAESec:    1.85,
		ModelVersion:         "fallback-heuristic-v1",
		FallbackUsed:         true,
		FallbackReason:       reason,
		BaselineClassMedian:  spec.HistoricalMeanSec,
		PredictionLatencyMs:  float64(elapsed.Microseconds()) / 1000.0,
		PredictedAt:          time.Now().UTC().Format(time.RFC3339),
	}
}

// FallbackBatch generates deterministic fallback predictions for a slice of tasks.
func FallbackBatch(tasks []workload.Task, reason string, elapsed time.Duration) []PredictionResult {
	out := make([]PredictionResult, len(tasks))
	for i, t := range tasks {
		out[i] = FallbackOne(t, reason, elapsed)
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
