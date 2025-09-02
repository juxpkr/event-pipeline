#!/bin/bash
set -e
set -x

# 기본 변수 설정
: ${DB_DRIVER:=derby}
SKIP_SCHEMA_INIT="${IS_RESUME:-false}"

# 스키마 초기화 함수
initialize_hive() {
  echo ">>>> [CUSTOM] Checking Hive schema version..."

  # -info 명령어가 스키마가 없어서 실패하더라도, set -e가 발동하지않도록 항상 성공한것처럼 만듬
  if /opt/hive/bin/schematool -dbType postgres -info; then
    # 스키마 존재 하면
    echo ">>>> [CUSTOM] Schema already exists. Upgrading if necessary..."
    /opt/hive/bin/schematool -dbType postgres -upgradeSchema
    if [ $? -ne 0 ]; then
        echo ">>>> [CUSTOM] Schema upgrade failed!"
        exit 1
    fi
  else
    # 스키마 없으면
    echo ">>>> [CUSTOM] Schema does not exist. Initializing schema..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
    if [ $? -ne 0 ]; then
        echo ">>>> [CUSTOM] Schema initialization failed!"
        exit 1
    fi
  fi
  echo ">>>> [CUSTOM] Hive schema is ready."
}

# 설정 디렉토리 및 HADOOP_CLIENT_OPTS 설정 (원본 스크립트 내용)
export HIVE_CONF_DIR=$HIVE_HOME/conf
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
  export TEZ_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"

# bash 문법으로 스키마 초기화 함수 호출
if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  initialize_hive
fi

# bash 문법으로 서비스별 설정
if [[ "${SERVICE_NAME}" == "hiveserver2" ]]; then
  export HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH
elif [[ "${SERVICE_NAME}" == "metastore" ]]; then
  export METASTORE_PORT=${METASTORE_PORT:-9083}
fi

# 최종 서비스 실행
exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service $SERVICE_NAME