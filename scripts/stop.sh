#!/bin/bash
echo ">>> Stopping Big Data Cluster (Freezing state)..."
docker stop big-data-control-plane
echo ">>> Cluster stopped. Run ./start_cluster.sh to resume."
