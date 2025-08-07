#!/bin/bash

# MSYS_NO_PATHCONV=1: Git Bash가 경로를 자동으로 변환하는 것을 막는다.
# 이렇게 해야 컨테이너 내부 경로인 /opt/spark/... 와 /app/spark-test.py가
# 윈도우 경로로 잘못 바뀌는 것을 완벽하게 방지할 수 있다.
MSYS_NO_PATHCONV=1 docker-compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.602 \
  /app/build_travel_datamart.py