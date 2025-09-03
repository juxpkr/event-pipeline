#!/bin/sh
# set -e 로 스크립트의 예측 가능성을 높인다
set -e

# PostgreSQL 서버가 준비되었는지 확인 (timeout 제거하고 docker-compose의 start_period로 제어)
pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q || exit 1

# metastore_db가 생성되었는지 확인
DB_METASTORE_READY=false
for i in $(seq 1 5); do
    # psql 명령이 실패해도 set -e가 스크립트를 중단시키지 않도록 `|| true` 추가
    DB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='metastore_db'" || true)
    
    if [ "$DB_EXISTS" = "1" ]; then
        echo "✅ metastore_db found."
        DB_METASTORE_READY=true
        break
    fi
    echo "Waiting for metastore_db... attempt $i/5"
    sleep 2
done

# superset DB가 생성되었는지 확인
DB_SUPERSET_READY=false
for i in $(seq 1 5); do
    DB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='superset'" || true)
    
    if [ "$DB_EXISTS" = "1" ]; then
        echo "✅ superset db found."
        DB_SUPERSET_READY=true
        break
    fi
    echo "Waiting for superset db... attempt $i/5"
    sleep 2
done

# 최종적으로 두 DB가 모두 준비되었는지 명시적으로 확인
if [ "$DB_METASTORE_READY" = "true" ] && [ "$DB_SUPERSET_READY" = "true" ]; then
    echo "PostgreSQL healthcheck passed!"
    exit 0
else
    echo "Healthcheck failed: One or more databases were not ready in time."
    exit 1
fi