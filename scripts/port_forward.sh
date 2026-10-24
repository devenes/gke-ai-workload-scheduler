#!/usr/bin/env bash
set -euo pipefail

K8S_NAMESPACE="${K8S_NAMESPACE:-ai-workload-scheduler}"
LOCAL_PORT="${LOCAL_PORT:-8080}"

echo "Forwarding http://127.0.0.1:${LOCAL_PORT} -> svc/scheduler-controller:8080 in namespace ${K8S_NAMESPACE}..."
echo "Note: This process runs in the foreground and terminates if the terminal closes. Re-run './scripts/port_forward.sh' to reconnect."
exec kubectl -n "${K8S_NAMESPACE}" port-forward svc/scheduler-controller "${LOCAL_PORT}:8080"
