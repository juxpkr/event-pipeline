# docker-compose.yml에서 받은 첫 번째 인자($1)를 확인
if [ "$1" = "master" ]; then
  # 인자가 "master"이면, 마스터 데몬을 실행
  exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$1" = "worker" ]; then
  # 인자가 "worker"이면, 워커 데몬을 실행
  # $2, $3는 docker-compose.yml에서 전달될 추가 인자 (예: spark://spark-master:7077)
  exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker "$2"
else
  # 그 외의 인자가 들어오면, 받은 인자를 그대로 명령어로 실행
  exec "$@"
fi