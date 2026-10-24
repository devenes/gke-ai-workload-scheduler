PROJECT_ID ?= enes-turan-demo
REGION ?= us-west4
ZONE ?= us-west4-a
CLUSTER_NAME ?= gke-ai-workload-scheduler
K8S_NAMESPACE ?= ai-workload-scheduler
IMAGE_TAG ?= v1.0.0
AR_REPO ?= $(REGION)-docker.pkg.dev/$(PROJECT_ID)/ai-workload-scheduler
SCHEDULER_IMAGE ?= $(AR_REPO)/scheduler:$(IMAGE_TAG)
PREDICTOR_IMAGE ?= $(AR_REPO)/predictor:$(IMAGE_TAG)

.PHONY: help test test-go test-py train-predictor simulate tf-validate tf-plan tf-apply build-images deploy port-forward verify-full verify-repro

help:
	@echo "GKE AI Workload Scheduler — Operational Targets"
	@echo "  make test            Run Go unit tests and Python predictor/runner pytest suites"
	@echo "  make train-predictor Train CPU-only scikit-learn runtime predictor and evaluate holdout"
	@echo "  make simulate        Run local 5-policy deterministic simulation comparison"
	@echo "  make tf-validate     Format and validate Terraform configuration"
	@echo "  make tf-plan         Generate Terraform execution plan"
	@echo "  make tf-apply        Provision GCP infrastructure via Terraform (persistent)"
	@echo "  make build-images    Build and push linux/amd64 container images to Artifact Registry"
	@echo "  make deploy          Deploy Kubernetes manifests to GKE cluster"
	@echo "  make port-forward    Port-forward scheduler Web UI and API on http://127.0.0.1:8080"
	@echo "  make verify-full     Execute full T00..T19 verification suite against live GKE"
	@echo "  make verify-repro    Execute safe reproducibility subset (T00,T01,T02,T08,T17,T18,T19)"

test: test-go test-py

test-go:
	go test -v ./...

test-py:
	.venv/bin/pytest -v

train-predictor:
	.venv/bin/python -m predictor.train --artifact-dir predictor/artifacts

simulate:
	go run ./cmd/scheduler -mode=simulate -local-mode=true -scenario=deadline_sensitive -seed=42 -task-count=24 -concurrency=4

tf-validate:
	cd infra/terraform && terraform fmt -check && terraform validate

tf-plan:
	cd infra/terraform && terraform plan -out=tfplan

tf-apply:
	cd infra/terraform && terraform apply "tfplan"

build-images:
	./scripts/deploy.sh --build-only

deploy:
	./scripts/deploy.sh

port-forward:
	./scripts/port_forward.sh

verify-full:
	.venv/bin/python scripts/run_test_suite.py --mode full

verify-repro:
	.venv/bin/python scripts/run_test_suite.py --mode repro
