#!/bin/bash
set -e

echo ">>> Starting Big Data Cluster..."
docker start big-data-control-plane

echo ">>> Waiting for Kubernetes API..."
# Wait loop for API server
until kubectl cluster-info >/dev/null 2>&1; do
    echo "Waiting for API server..."
    sleep 2
done

echo ">>> Cluster Online! Wait for them to be ready (60s)..."
sleep 60
echo "Status of pods:"
kubectl get pods -n big-data
