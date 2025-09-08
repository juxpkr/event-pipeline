#!/bin/sh

# Master 데몬을 위한 Java 옵션
export SPARK_MASTER_OPTS="$SPARK_MASTER_OPTS -javaagent:/opt/spark/jars/jmx_prometheus_javaagent-1.4.0.jar=8090:/opt/spark/conf/jmx-config.yml"

# Worker 데몬을 위한 Java 옵션
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -javaagent:/opt/spark/jars/jmx_prometheus_javaagent-1.4.0.jar=8091:/opt/spark/conf/jmx-config.yml"
