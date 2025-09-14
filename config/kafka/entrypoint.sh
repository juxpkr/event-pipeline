#!/bin/bash
set -e

echo "Step 1: Waiting for ZooKeeper..."
cub zk-ready zookeeper:2181 60
echo "✅ Step 1 Success."

echo "Step 2: Removing stale ZK node..."
/usr/bin/zookeeper-shell.sh zookeeper:2181 rmr /brokers/ids/$KAFKA_BROKER_ID || true
echo "✅ Step 2 Complete."

echo "Step 3: Starting Kafka broker via exec..."
exec /etc/confluent/docker/run