package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var dnsLabelRegex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Config holds validated runtime configuration for the GKE AI Workload Scheduler.
type Config struct {
	ProjectID          string `json:"project_id"`
	Region             string `json:"region"`
	Zone               string `json:"zone"`
	ClusterName        string `json:"cluster_name"`
	Namespace          string `json:"namespace"`
	HTTPAddr           string `json:"http_addr"`
	PredictorURL       string `json:"predictor_url"`
	WorkerImage        string `json:"worker_image"`
	ServiceAccountName string `json:"service_account_name"`
	StateDir           string `json:"state_dir"`
	ArtifactsDir       string `json:"artifacts_dir"`
	GCSBucket          string `json:"gcs_bucket"`
	DefaultPolicy      string `json:"default_policy"`
	MaxConcurrentJobs  int    `json:"max_concurrent_jobs"`
	MaxCPUBudgetMilli  int    `json:"max_cpu_budget_milli"`
	MaxMemoryBudgetMB  int    `json:"max_memory_budget_mb"`
	LocalMode          bool   `json:"local_mode"`
	EnableCRDStore     bool   `json:"enable_crd_store"`
	AppImageDigest     string `json:"app_image_digest"`
}

// Default returns safe baseline settings matching the project specification.
func Default() Config {
	return Config{
		ProjectID:          "your-gcp-project-id",
		Region:             "us-west4",
		Zone:               "us-west4-a",
		ClusterName:        "gke-ai-workload-scheduler",
		Namespace:          "ai-workload-scheduler",
		HTTPAddr:           ":8080",
		PredictorURL:       "http://127.0.0.1:8081",
		WorkerImage:        "us-west4-docker.pkg.dev/your-gcp-project-id/ai-workload-scheduler/scheduler:v1.0.0",
		ServiceAccountName: "scheduler-worker-sa",
		StateDir:           "artifacts/state",
		ArtifactsDir:       "artifacts",
		GCSBucket:          "",
		DefaultPolicy:      "ai_assisted",
		MaxConcurrentJobs:  4,
		MaxCPUBudgetMilli:  2000,
		MaxMemoryBudgetMB:  4096,
		LocalMode:          false,
		EnableCRDStore:     true,
		AppImageDigest:     "local-dev-build",
	}
}

// FromEnv loads configuration from environment variables over Default().
func FromEnv() Config {
	cfg := Default()
	if v := strings.TrimSpace(os.Getenv("PROJECT_ID")); v != "" {
		cfg.ProjectID = v
	}
	if v := strings.TrimSpace(os.Getenv("REGION")); v != "" {
		cfg.Region = v
	}
	if v := strings.TrimSpace(os.Getenv("ZONE")); v != "" {
		cfg.Zone = v
	}
	if v := strings.TrimSpace(os.Getenv("CLUSTER_NAME")); v != "" {
		cfg.ClusterName = v
	}
	if v := strings.TrimSpace(os.Getenv("K8S_NAMESPACE")); v != "" {
		cfg.Namespace = v
	}
	if v := strings.TrimSpace(os.Getenv("HTTP_ADDR")); v != "" {
		cfg.HTTPAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("PREDICTOR_URL")); v != "" {
		cfg.PredictorURL = v
	}
	if v := strings.TrimSpace(os.Getenv("WORKER_IMAGE")); v != "" {
		cfg.WorkerImage = v
	}
	if v := strings.TrimSpace(os.Getenv("WORKER_SERVICE_ACCOUNT")); v != "" {
		cfg.ServiceAccountName = v
	}
	if v := strings.TrimSpace(os.Getenv("STATE_DIR")); v != "" {
		cfg.StateDir = v
	}
	if v := strings.TrimSpace(os.Getenv("ARTIFACTS_DIR")); v != "" {
		cfg.ArtifactsDir = v
	}
	if v := strings.TrimSpace(os.Getenv("GCS_BUCKET")); v != "" {
		cfg.GCSBucket = v
	}
	if v := strings.TrimSpace(os.Getenv("DEFAULT_POLICY")); v != "" {
		cfg.DefaultPolicy = v
	}
	if v := strings.TrimSpace(os.Getenv("MAX_CONCURRENT_JOBS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxConcurrentJobs = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("MAX_CPU_BUDGET_MILLI")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxCPUBudgetMilli = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("MAX_MEMORY_BUDGET_MB")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxMemoryBudgetMB = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("LOCAL_MODE")); v != "" {
		cfg.LocalMode = v == "true" || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("ENABLE_CRD_STORE")); v != "" {
		cfg.EnableCRDStore = v == "true" || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("APP_IMAGE_DIGEST")); v != "" {
		cfg.AppImageDigest = v
	}
	return cfg
}

// Validate checks bounds and formatting of configuration fields.
func (c Config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("project_id is required")
	}
	if !IsValidDNSLabel(c.Namespace) {
		return fmt.Errorf("invalid namespace %q: must be RFC 1123 DNS label", c.Namespace)
	}
	if c.MaxConcurrentJobs < 1 || c.MaxConcurrentJobs > 16 {
		return fmt.Errorf("max_concurrent_jobs (%d) must be in [1, 16]", c.MaxConcurrentJobs)
	}
	if c.MaxCPUBudgetMilli < 100 || c.MaxCPUBudgetMilli > 16000 {
		return fmt.Errorf("max_cpu_budget_milli (%d) must be in [100, 16000]", c.MaxCPUBudgetMilli)
	}
	if c.MaxMemoryBudgetMB < 64 || c.MaxMemoryBudgetMB > 32768 {
		return fmt.Errorf("max_memory_budget_mb (%d) must be in [64, 32768]", c.MaxMemoryBudgetMB)
	}
	return nil
}

// IsValidDNSLabel checks whether s is a valid RFC 1123 DNS label.
func IsValidDNSLabel(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	return dnsLabelRegex.MatchString(s)
}
