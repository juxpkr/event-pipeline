#!/bin/sh

echo "Waiting for Prometheus..."
until nc -zv prometheus 9090; do
    >&2 echo "Prometheus is unavailable - sleeping"
    sleep 5
done

echo "Prometheus is up - starting Grafana."
# Grafana의 원래 시작 스크립트 실행
exec /run.sh