# Java 환경 확인
echo "JAVA_HOME: $JAVA_HOME"
echo "Java version:"
java -version

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
  echo "Starting Spark Worker..."
  /opt/spark/sbin/start-worker.sh "$2" &
  exec tail -f /dev/null

elif [ "${ROLE}" = "thrift-http" ]; then
  # 역할이 "thrift-http"이면, HTTP 모드로 Thrift Server를 실행
  # Master가 완전히 시작될 때까지 30초간 대기
  echo "Waiting 30 seconds for Spark Master to be fully ready..."
  sleep 30

  # 30초 후 Thrift 서버 시작
  echo "Starting Spark Thrift Server in HTTP mode..."
  # 컨테이너가 꺼지지 않도록 tail과 함께 실행
  /opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 &
  exec tail -f /dev/null

else
  # 그 외의 인자가 들어오면, 받은 인자를 그대로 명령어로 실행
  echo "Executing command: $@"
  exec "$@"
fi