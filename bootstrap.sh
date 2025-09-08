#!/bin/bash

set -euo pipefail

AWS_PROFILE="bootstrap"
REGION="us-east-1"         
CLUSTER_NAME="infra-ops" 

terraform apply

log "Waiting for EKS cluster '$CLUSTER_NAME' to become ACTIVE..."
ATTEMPTS=0
MAX_ATTEMPTS=120

while true; do
  STATUS="$(aws eks describe-cluster --name "$CLUSTER_NAME" --region "$REGION" --query 'cluster.status' --output text 2>/dev/null || echo "UNKNOWN")"
  if [[ "$STATUS" == "ACTIVE" ]]; then
    log "Cluster is ACTIVE."
    break
  fi

  ((ATTEMPTS++))
  if (( ATTEMPTS > MAX_ATTEMPTS )); then
    echo "Timed out waiting for cluster to become ACTIVE. Last status: $STATUS" >&2
    exit 1
  fi

  echo -n "."
  sleep 10
done

log "Updating kubeconfig..."
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$REGION" ${AWS_PROFILE:+--profile "$AWS_PROFILE"}

log "Verifying cluster access..."
kubectl cluster-info
kubectl get nodes -o wide
