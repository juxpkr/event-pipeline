#!/bin/sh
set -e

# docker-compose에서 전달받은 환경변수 사용
KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}

echo "Using Kafka bootstrap server: $KAFKA_BOOTSTRAP_SERVER"
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b $KAFKA_BOOTSTRAP_SERVER 1 60

echo "Kafka is ready! Creating topics..."

# Create GDELT events topic
kafka-topics --create \
    --topic gdelt_events \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP_SERVER

# Create Wiki events topic  
kafka-topics --create \
    --topic wiki_events \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP_SERVER

echo "Topics created successfully!"

# List topics to verify
echo "Available topics:"
kafka-topics --list --bootstrap-server $KAFKA_BOOTSTRAP_SERVER