#!/bin/bash
set -e

echo ">>> Stopping all services (Force Kill)..."
# Scale down immediately (no grace period needed since we wipe data)
kubectl -n big-data delete deploy,statefulset --all --force --grace-period=0

echo ">>> Waiting for pods to terminate..."
kubectl -n big-data wait --for=delete pod --all --timeout=60s || echo "Pods deleted"

echo ">>> Wiping Data (PVCs)..."
# This deletes Kafka logs, Cassandra data, and Spark checkpoints
kubectl -n big-data delete pvc --all

echo ">>> Restarting Services..."
# Re-apply manifests to reset replicas to 1
kubectl apply -f kubernetes/00-core.yaml # Ensure core/namespace
echo ">>> Regenerating ConfigMap..."
kubectl -n big-data create configmap cassandra-init --from-file=init.cql=cassandra/init.cql --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f kubernetes/01-data-layer.yaml
kubectl apply -f kubernetes/05-hdfs.yaml

echo ">>> Waiting for Data Layer..."
kubectl -n big-data rollout status statefulset/broker
kubectl -n big-data rollout status statefulset/cassandra
kubectl -n big-data rollout status statefulset/namenode
kubectl -n big-data rollout status statefulset/datanode

echo ">>> Re-initializing Schema & Topics..."
# Delete old job so we can re-run it
kubectl -n big-data delete job init-cassandra init-kafka --ignore-not-found
kubectl apply -f kubernetes/02-init-job.yaml
kubectl -n big-data wait --for=condition=complete --timeout=120s job/init-cassandra
kubectl -n big-data wait --for=condition=complete --timeout=60s job/init-kafka

echo ">>> Restarting Apps..."
kubectl apply -f kubernetes/03-apps.yaml
kubectl apply -f kubernetes/04-ui.yaml

echo ">>> Reset Complete! System is fresh."
