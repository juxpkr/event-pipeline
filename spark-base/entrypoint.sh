#!/bin/bash

# 동적 Spark 클러스터 시작 스크립트 
# docker-compose.yaml에서 SPARK_DYNAMIC_ 접두사가 붙은 환경변수들을 읽어서
# spark-defaults.conf 파일에 동적으로 설정을 주입한 다음
# 지정된 Spark 컴포넌트(Master, Worker, Thrift Server)를 실행한다.

# 기본 환경 확인
echo " Entrypoint execution started."
echo "JAVA_HOME: $JAVA_HOME"
java -version

# Spark Submit 레벨에서 PostgreSQL 드라이버 강제 등록 (안정성을 위해)
export SPARK_SUBMIT_OPTS="-Djdbc.drivers=org.postgresql.Driver"
echo "SPARK_SUBMIT_OPTS: $SPARK_SUBMIT_OPTS"


# 동적 설정 주입 
# master, worker 등을 실행하기 전에, 공통으로 설정을 먼저 주입한다.
CONFIG_FILE="/opt/spark/conf/spark-defaults.conf"

echo "" >> ${CONFIG_FILE}
echo "# =========================================================" >> ${CONFIG_FILE}
echo "# Dynamically injected configurations from docker-compose" >> ${CONFIG_FILE}
echo "# Generated at: $(date)" >> ${CONFIG_FILE}
echo "# =========================================================" >> ${CONFIG_FILE}

# 'SPARK_DYNAMIC_'으로 시작하는 모든 환경변수를 루프 처리
for VAR in $(env | grep "^SPARK_DYNAMIC_"); do
  # 'SPARK_DYNAMIC_' 접두사를 제거해서 Spark 설정 키 생성 (예: worker_cores -> worker.cores)
  KEY=$(echo "${VAR}" | sed -e 's/SPARK_DYNAMIC_//' -e 's/=.*//' | tr '_' '.')
  # '=' 뒤의 값 추출
  VALUE=$(echo "${VAR}" | sed -e 's/.*=//')

  # 최종적으로 spark-defaults.conf 파일에 "spark.key value" 형식으로 추가
  echo "spark.${KEY}   ${VALUE}" >> ${CONFIG_FILE}
  echo " Injected Config: spark.${KEY}=${VALUE}"
done
echo "# --- End of dynamic configurations ---"
echo "" >> ${CONFIG_FILE}

# 디버깅을 위해 최종 설정 파일 내용 출력
echo " Final spark-defaults.conf content:"
cat ${CONFIG_FILE}
echo "-----------------------------------------"


# docker-compose에서 받은 첫 번째 인자($1)를 ROLE 변수에 저장
ROLE=$1
echo "Executing entrypoint with role: ${ROLE}"

# ROLE 값에 따라 다른 명령을 수행
if [ "${ROLE}" = "master" ]; then
  # 역할이 "master"이면, Spark Master를 실행
  echo "Starting Spark Master..."
  /opt/spark/sbin/start-master.sh &
  exec tail -f /dev/null

elif [ "${ROLE}" = "worker" ]; then
  # 역할이 "worker"이면, Spark Worker를 실행
  # $2는 docker-compose.yml에서 전달될 Master URL
  echo "Starting Spark Worker for Master at $2..."
  /opt/spark/sbin/start-worker.sh "$2" &
  exec tail -f /dev/null

elif [ "${ROLE}" = "thrift-binary" ]; then
  # 역할이 "thrift-binary"이면, Binary 모드로 Thrift Server를 실행
  echo "Starting Spark Thrift Server in BINARY mode..."
  /opt/spark/sbin/start-thriftserver.sh \
    --master spark://spark-master:7077 &
  exec tail -f /dev/null

else
  # 그 외의 인자가 들어오면, 받은 인자를 그대로 명령어로 실행
  echo "Executing command: $@"
  exec "$@"
fi