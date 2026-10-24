#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-enes-turan-demo}"
REGION="${REGION:-us-west4}"
ZONE="${ZONE:-us-west4-a}"
CLUSTER_NAME="${CLUSTER_NAME:-gke-ai-workload-scheduler}"
K8S_NAMESPACE="${K8S_NAMESPACE:-ai-workload-scheduler}"
IMAGE_TAG="${IMAGE_TAG:-v1.0.0}"
AR_REPO="${REGION}-docker.pkg.dev/${PROJECT_ID}/ai-workload-scheduler"

export CLOUDSDK_PYTHON="${CLOUDSDK_PYTHON:-/opt/homebrew/bin/python3.13}"

echo "[1/4] Configuring Docker authentication for ${REGION}-docker.pkg.dev..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "[2/4] Building and pushing linux/amd64 images to ${AR_REPO}..."
docker buildx build --platform linux/amd64 \
  -t "${AR_REPO}/scheduler:${IMAGE_TAG}" \
  -f Dockerfile . --push

docker buildx build --platform linux/amd64 \
  -t "${AR_REPO}/predictor:${IMAGE_TAG}" \
  -f predictor/Dockerfile . --push

SCHEDULER_DIGEST=$(gcloud artifacts docker images describe "${AR_REPO}/scheduler:${IMAGE_TAG}" --format='value(image_summary.digest)')
PREDICTOR_DIGEST=$(gcloud artifacts docker images describe "${AR_REPO}/predictor:${IMAGE_TAG}" --format='value(image_summary.digest)')

echo "  Scheduler Image: ${AR_REPO}/scheduler@${SCHEDULER_DIGEST}"
echo "  Predictor Image: ${AR_REPO}/predictor@${PREDICTOR_DIGEST}"

mkdir -p artifacts
cat > artifacts/deployed_images.json <<EOF
{
  "project_id": "${PROJECT_ID}",
  "region": "${REGION}",
  "repository": "${AR_REPO}",
  "tag": "${IMAGE_TAG}",
  "scheduler_image": "${AR_REPO}/scheduler:${IMAGE_TAG}",
  "scheduler_digest": "${SCHEDULER_DIGEST}",
  "scheduler_pinned": "${AR_REPO}/scheduler@${SCHEDULER_DIGEST}",
  "predictor_image": "${AR_REPO}/predictor:${IMAGE_TAG}",
  "predictor_digest": "${PREDICTOR_DIGEST}",
  "predictor_pinned": "${AR_REPO}/predictor@${PREDICTOR_DIGEST}"
}
EOF

if [[ "${1:-}" == "--build-only" ]]; then
  exit 0
fi

echo "[3/4] Fetching GKE cluster credentials for ${CLUSTER_NAME} (${ZONE})..."
gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"

echo "[4/4] Rendering Kustomize manifests with pinned digests and applying to ${K8S_NAMESPACE}..."
TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT
cp -R deploy "${TMP_DIR}/deploy"

sed -i.bak \
  -e "s|us-west4-docker.pkg.dev/enes-turan-demo/ai-workload-scheduler/scheduler:v1.0.0|${AR_REPO}/scheduler@${SCHEDULER_DIGEST}|g" \
  -e "s|value: \"pinned-at-deploy\"|value: \"${SCHEDULER_DIGEST}\"|g" \
  "${TMP_DIR}/deploy/base/controller-deployment.yaml"

sed -i.bak \
  -e "s|us-west4-docker.pkg.dev/enes-turan-demo/ai-workload-scheduler/predictor:v1.0.0|${AR_REPO}/predictor@${PREDICTOR_DIGEST}|g" \
  -e "s|value: \"pinned-at-deploy\"|value: \"${PREDICTOR_DIGEST}\"|g" \
  "${TMP_DIR}/deploy/base/predictor-deployment.yaml"

mkdir -p dist/k8s
kubectl kustomize "${TMP_DIR}/deploy/overlays/gke" > dist/k8s/gke-manifests.yaml
kubectl apply -f dist/k8s/gke-manifests.yaml

kubectl -n "${K8S_NAMESPACE}" rollout status deployment/runtime-predictor --timeout=180s
kubectl -n "${K8S_NAMESPACE}" rollout status deployment/scheduler-controller --timeout=180s
kubectl -n "${K8S_NAMESPACE}" get pods,svc,deployments,podmonitoring -o wide
