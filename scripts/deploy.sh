#!/bin/bash
set -e

echo ">>> Creating Kind Cluster..."
kind create cluster --name big-data || echo "Cluster exists"

echo ">>> Building Images (baking data/code)..."
docker build -t big-data-project-producer:latest -f kafka/Dockerfile .
docker build -t big-data-project-spark:latest -f spark/Dockerfile .

echo ">>> Loading Images into Kind..."
kind load docker-image big-data-project-producer:latest --name big-data
kind load docker-image big-data-project-spark:latest --name big-data
# Kafka, Cassandra, Grafana, Kafka-UI are pulled from Docker Hub by Kind automatically

echo ">>> Applying Manifests..."
# Apply sequentially to ensure namespace exists first
kubectl apply -f kubernetes/00-core.yaml

echo ">>> Generating ConfigMap from init.cql..."
kubectl -n big-data create configmap cassandra-init --from-file=init.cql=cassandra/init.cql --dry-run=client -o yaml | kubectl apply -f -

echo ">>> Generating Grafana Dashboard ConfigMaps..."
# Dashboard provisioning config
kubectl -n big-data create configmap grafana-dashboard-config \
  --from-file=dashboard.yaml=grafana/provisioning/dashboards/dashboard.yaml \
  --dry-run=client -o yaml | kubectl apply -f -

# All dashboard JSON files (load all *.json files from the dashboards folder)
DASHBOARD_FILES=""
for f in grafana/provisioning/dashboards/*.json; do
  DASHBOARD_FILES="$DASHBOARD_FILES --from-file=$(basename $f)=$f"
done
kubectl -n big-data create configmap grafana-dashboards $DASHBOARD_FILES --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f kubernetes/01-data-layer.yaml

echo ">>> Deploying HDFS (Batch Layer)..."
kubectl apply -f kubernetes/05-hdfs.yaml

echo ">>> Waiting for Data Layer (Cassandra/Kafka/HDFS)..."
kubectl -n big-data rollout status statefulset/broker
kubectl -n big-data rollout status statefulset/cassandra
kubectl -n big-data rollout status statefulset/namenode
kubectl -n big-data rollout status statefulset/datanode

echo ">>> Running Init Jobs (Cassandra & Kafka)..."
# Delete old jobs if they exist to allow re-run
kubectl -n big-data delete job init-cassandra init-kafka --ignore-not-found
kubectl apply -f kubernetes/02-init-job.yaml
kubectl -n big-data wait --for=condition=complete --timeout=120s job/init-cassandra
kubectl -n big-data wait --for=condition=complete --timeout=60s job/init-kafka

echo ">>> Deploying Apps..."
kubectl apply -f kubernetes/03-apps.yaml
kubectl apply -f kubernetes/04-ui.yaml

echo ">>> Done! Access services:"
echo "Grafana: http://localhost:30300 (via NodePort - requires port forward if on mac/remote)"
echo "Kafka UI: http://localhost:30080"
echo "HDFS UI: kubectl -n big-data port-forward svc/namenode 9870:9870"
echo "Metrics: kubectl -n big-data port-forward svc/grafana 3000:3000"
