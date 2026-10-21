#!/usr/bin/env bash
# Creates a k3d cluster running an Argo CD release with the ApplicationSync CRDs installed, for make start-local.
set -euo pipefail

CLUSTER=${CLUSTER:-argo-cd-sync-controller}
ARGOCD_VERSION=${ARGOCD_VERSION:?set ARGOCD_VERSION, or run make start-test-k8s}

if k3d cluster get "$CLUSTER" >/dev/null 2>&1; then
  k3d cluster start "$CLUSTER"
else
  k3d cluster create "$CLUSTER" --wait
fi
kubectl config use-context "k3d-$CLUSTER"

kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side --force-conflicts -n argocd \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"
kubectl apply --server-side -k config/crd
kubectl -n argocd rollout status deployment/argocd-server --timeout=5m
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=5m

cat <<MSG
Argo CD ${ARGOCD_VERSION} is running in k3d cluster ${CLUSTER}.
Run 'make start-local' for the controller, and the UI on https://localhost:8443 (user admin, password from
'argocd admin initial-password -n argocd'). 'kubectl apply -k config/samples' adds example apps.
MSG
