#!/bin/sh
set -e

# 반복되는 로직은 함수로 만들자.
create_db_if_not_exists() {
  local DBNAME=$1
  # psql 명령어로 DB 목록을 조회해서, 내가 찾는 DB 이름이 있는지 확인한다.
  # -t: 헤더 없이 결과만 출력, -A: 정렬 없이 출력, -c: 명령어 실행
  if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='$DBNAME'" | grep -q 1; then
    echo "Database '$DBNAME' already exists. Skipping."
  else
    echo "Database '$DBNAME' does not exist. Creating..."
    # 존재하지 않을 때만 생성 명령을 실행한다.
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE $DBNAME"
  fi
}

echo "Setting up databases..."
# 함수를 호출해서 각 DB를 체크하고 생성한다.
create_db_if_not_exists "metastore_db"
create_db_if_not_exists "superset"

# 권한 부여는 DB 생성이 확실히 끝난 뒤에 실행한다.
echo "Granting privileges..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    GRANT ALL PRIVILEGES ON DATABASE metastore_db TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE superset TO $POSTGRES_USER;
EOSQL

echo "Databases and privileges are set up successfully!"