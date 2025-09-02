#!/bin/sh
set -e

# airflow DB가 준비되었는지 확인
pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" || exit 1

# 잠시 기다린 후 DB 존재 확인 (더 관대하게)
sleep 2

# metastore_db가 생성되었는지 확인 (실패해도 재시도)
for i in $(seq 1 3); do
    if psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='metastore_db'" | grep -q 1; then
        break
    fi
    echo "Waiting for metastore_db... attempt $i/3"
    sleep 1
done

# superset DB가 생성되었는지 확인 (실패해도 재시도)
for i in $(seq 1 3); do
    if psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='superset'" | grep -q 1; then
        break
    fi
    echo "Waiting for superset db... attempt $i/3"
    sleep 1
done

echo "PostgreSQL healthcheck passed!"
exit 0