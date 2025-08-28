# Java 환경 확인
echo "JAVA_HOME: $JAVA_HOME"
echo "Java version:"
java -version

# PostgreSQL JDBC 드라이버를 시스템 클래스패스에 추가 (DataNucleus ClassLoader 문제 해결)
export CLASSPATH="/opt/spark/jars/postgresql-42.7.3.jar:$CLASSPATH"
echo "CLASSPATH updated: $CLASSPATH"

# Spark Submit 레벨에서 PostgreSQL 드라이버 강제 등록
export SPARK_SUBMIT_OPTS="-Djdbc.drivers=org.postgresql.Driver"
echo "SPARK_SUBMIT_OPTS: $SPARK_SUBMIT_OPTS"

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
  echo "Starting Spark Thrift Server in BINARY mode with all packages..."
  # 컨테이너가 꺼지지 않도록 tail과 함께 실행
  exec /opt/spark/bin/spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --master spark://spark-master:7077 \
    --packages "io.delta:delta-core_2.12:${DELTA_SPARK_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION},org.postgresql:postgresql:${POSTGRESQL_JDBC_VERSION}" \
    --conf spark.driver.extraClassPath=/opt/spark/jars/postgresql-42.7.3.jar \
    --conf spark.hive.server2.transport.mode=binary \
    --conf spark.hive.server2.thrift.port=10001 \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.cores.max=2 \
    --conf spark.executor.memory=1g \
    anything

else
  # 그 외의 인자가 들어오면, 받은 인자를 그대로 명령어로 실행
  echo "Executing command: $@"
  exec "$@"
fi