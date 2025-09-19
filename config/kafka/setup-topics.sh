#!/bin/sh
set -e

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 60

echo "Kafka is ready! Creating topics..."

# Create GDELT events topic
kafka-topics --create \
    --topic gdelt_events \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --bootstrap-server kafka:9092

# Create Wiki events topic  
kafka-topics --create \
    --topic wiki_events \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists \
    --bootstrap-server kafka:9092

echo "Topics created successfully!"

# List topics to verify
echo "Available topics:"
kafka-topics --list --bootstrap-server kafka:9092