#!/bin/bash
set -e

# 초기화 완료 플래그 파일 경로
INIT_FLAG_FILE="/app/superset_home/.initialized"

# 1. Postgres 연결 체크 (wait-for-it.sh로 이미 처리됨)
echo "Postgres connection already verified by wait-for-it.sh"
# until sh -c 'cat < /dev/null > /dev/tcp/postgres/5432'; do
#     >&2 echo "Postgres is unavailable - sleeping"
#     sleep 5
# done

# 2. 매번 초기화 실행 (권한 문제 방지)
echo "Initializing Superset (every startup for stability)..."

# Upgrade Superset database
echo "Upgrading Superset database..."
superset db upgrade

# Create admin user (실패해도 무시 - 이미 존재할 수 있음)
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin || echo "Admin user already exists, continuing..."

# Initialize Superset (권한 동기화)
echo "Initializing Superset permissions..."
superset init

# 3. Superset 웹서버 실행 (매번 실행) 
echo "Starting Superset webserver..."
# exec를 사용하면 이 스크립트가 웹서버 프로세스 자체로 대체되어, Docker가 프로세스를 더 잘 관리할 수 있음.
exec superset run -h 0.0.0.0 -p 8088