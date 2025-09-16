#!/bin/bash

set -e

echo "Phase 1: Waiting for ZooKeeper service to be fully ready..."
cub zk-ready zookeeper:2181 60
echo "✅ Phase 1 Complete."

echo "Phase 2: Removing potentially stale ZooKeeper node for this broker..."
# 유령 노드를 삭제한다.
# || true: 노드가 없어서 에러가 나도 무시하고 계속 진행하는 안전장치.
echo "rmr /brokers/ids/$KAFKA_BROKER_ID" | /opt/kafka/bin/zookeeper-shell.sh zookeeper:2181 || true
echo "✅ Phase 2 Complete."

echo "Phase 3: Starting the Kafka broker..."
exec /etc/confluent/docker/run