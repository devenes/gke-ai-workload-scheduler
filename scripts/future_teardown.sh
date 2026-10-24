#!/usr/bin/env bash
# ==============================================================================
# FUTURE SEPARATELY-AUTHORIZED TEARDOWN PROCEDURE
# ==============================================================================
# CRITICAL: The GKE AI Workload Scheduler environment is intentionally kept
# running per the project specification. Do NOT execute this script unless the
# project owner has issued an explicit, separate authorization to decommission
# the environment.
# ==============================================================================
set -euo pipefail

if [[ "${1:-}" != "--confirm-authorized-destroy" ]]; then
  echo "BLOCKED: This environment is configured to remain running persistently."
  echo "Do NOT run teardown during implementation, benchmarking, or verification."
  echo "If you have received separate explicit authorization to destroy the environment in the future, run:"
  echo "  ./scripts/future_teardown.sh --confirm-authorized-destroy"
  exit 1
fi

echo "Authorized teardown requested. Destroying Terraform-managed resources..."
cd "$(dirname "$0")/../infra/terraform"
terraform destroy -auto-approve
