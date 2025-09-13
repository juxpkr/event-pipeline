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

# 2. 최초 실행인지 확인하고, 초기화 작업 수행
if [ ! -f "$INIT_FLAG_FILE" ]; then
    echo "This is the first run. Initializing Superset..."

    # Upgrade Superset database
    echo "Upgrading Superset database..."
    superset db upgrade

    # Create admin user
    echo "Creating admin user..."
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@superset.com \
        --password admin

    # Initialize Superset
    echo "Initializing Superset..."
    superset init

    # 초기화 완료 플래그 생성
    echo "Initialization completed. Creating flag file."
    touch "$INIT_FLAG_FILE"
else
    echo "Initialization already completed. Skipping..."
fi

# 3. Superset 웹서버 실행 (매번 실행) 
echo "Starting Superset webserver..."
# exec를 사용하면 이 스크립트가 웹서버 프로세스 자체로 대체되어, Docker가 프로세스를 더 잘 관리할 수 있음.
exec superset run -h 0.0.0.0 -p 8088