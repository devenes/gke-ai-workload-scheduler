package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/api"
	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/kubernetes"
	"github.com/devenes/gke-ai-workload-scheduler/internal/observability"
	"github.com/devenes/gke-ai-workload-scheduler/internal/predictor"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/scheduler"
)

func main() {
	cfg := config.FromEnv()

	var (
		mode        = flag.String("mode", "server", "Execution mode: server | simulate")
		httpAddr    = flag.String("http-addr", cfg.HTTPAddr, "HTTP listen address")
		localMode   = flag.Bool("local-mode", cfg.LocalMode, "Run without requiring a live Kubernetes cluster")
		predURL     = flag.String("predictor-url", cfg.PredictorURL, "Runtime predictor service base URL")
		scenario    = flag.String("scenario", "deadline_sensitive", "Scenario for CLI simulation mode")
		seed        = flag.Int64("seed", 42, "Deterministic seed for CLI simulation mode")
		taskCount   = flag.Int("task-count", 24, "Task count for CLI simulation mode")
		concurrency = flag.Int("concurrency", cfg.MaxConcurrentJobs, "Concurrency limit")
	)
	flag.Parse()

	cfg.HTTPAddr = *httpAddr
	cfg.LocalMode = *localMode
	cfg.PredictorURL = *predURL
	if *concurrency > 0 {
		cfg.MaxConcurrentJobs = *concurrency
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	var kubeClient *kubernetes.Client
	if !cfg.LocalMode {
		kc, err := kubernetes.NewClient(cfg)
		if err != nil {
			logger.Warn("kubernetes client unavailable; continuing with local simulation capabilities", "error", err)
			kubeClient = &kubernetes.Client{}
		} else {
			kubeClient = kc
		}
	}

	var syncer queue.KubeStateSyncer
	if kubeClient != nil && kubeClient.IsConnected() {
		syncer = kubeClient
	}

	store, err := queue.NewStore(cfg.StateDir, cfg.ArtifactsDir, syncer)
	if err != nil {
		logger.Error("failed to initialize durable queue store", "error", err)
		os.Exit(1)
	}

	predClient := predictor.NewClient(cfg.PredictorURL, 2500*time.Millisecond)
	rec := observability.NewRecorder()
	engine := scheduler.NewEngine(cfg, store, predClient, kubeClient, rec)

	if *mode == "simulate" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		report, err := engine.CompareAllPoliciesSimulation(ctx, *scenario, *seed, *taskCount, cfg.MaxConcurrentJobs)
		if err != nil {
			logger.Error("simulation failed", "error", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return
	}

	srv := api.NewServer(cfg, store, engine, predClient, kubeClient, rec)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting gke-ai-workload-scheduler server",
			"addr", cfg.HTTPAddr,
			"project_id", cfg.ProjectID,
			"namespace", cfg.Namespace,
			"k8s_connected", kubeClient != nil && kubeClient.IsConnected(),
			"predictor_url", cfg.PredictorURL,
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down server")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutCtx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}
}
