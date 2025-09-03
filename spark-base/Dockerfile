# -- [Builder] - 느리고 변하지 않는 작업을 여기서 수행 -- 
# 1. Python 3.10 slim 버전으로 시작한다.
FROM python:3.10-slim-bookworm AS builder

# 2. 작성자 정보를 남긴다 
LABEL maintainer="Juseong"

# 3. 필요한 환경 변수들을 미리 설정한다.
ARG SPARK_VERSION
ARG HADOOP_VERSION
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# PySpark가 Worker에서 Driver와 동일한 Python을 사용하도록 경로를 명시
ENV PYSPARK_PYTHON=/usr/local/bin/python 

# 4. 필요한 기본 도구와 Java를 설치한다.
#    --no-install-recommends는 불필요한 추천 패키지를 설치하지 않아 용량을 줄여준다.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 5. Spark 3.4.3 버전을 다운로드하고, 압축을 풀고, 정리한다.
#    (내장된 Python 버전과 상관없이 Python 3.10 환경 위에 설치됨)
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# S3 및 Delta Lake 통신에 필요한 모든 JAR 파일을 중앙에서 관리
# Build arguments 추가
ARG DELTA_SPARK_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRESQL_JDBC_VERSION
ARG KAFKA_CLIENTS_VERSION
ARG SCALA_VERSION
ARG COMMONS_POOL2_VERSION
ARG LZ4_JAVA_VERSION
ARG COMMONS_LOGGING_VERSION

RUN curl -o ${SPARK_HOME}/jars/delta-core_${SCALA_VERSION}-${DELTA_SPARK_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_VERSION}/${DELTA_SPARK_VERSION}/delta-core_${SCALA_VERSION}-${DELTA_SPARK_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/delta-storage-${DELTA_SPARK_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/commons-logging-${COMMONS_LOGGING_VERSION}.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/${COMMONS_LOGGING_VERSION}/commons-logging-${COMMONS_LOGGING_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar && \
    curl -o ${SPARK_HOME}/jars/lz4-java-${LZ4_JAVA_VERSION}.jar https://repo1.maven.org/maven2/org/lz4/lz4-java/${LZ4_JAVA_VERSION}/lz4-java-${LZ4_JAVA_VERSION}.jar

# BoneCP용 PostgreSQL JAR를 시스템 전역에 복사 (Java 17 호환)
RUN mkdir -p /usr/share/java && \
    cp ${SPARK_HOME}/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar /usr/share/java/ && \
    cp ${SPARK_HOME}/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar /usr/lib/jvm/java-17-openjdk-amd64/lib/

# -- Builder에서 결과물만 빠르게 복사 -- 
FROM python:3.10-slim-bookworm

# 작성자, 환경 변수 등은 최종 이미지에 다시 한번 정의해준다.
LABEL maintainer="Juseong"
ARG SPARK_VERSION
ARG HADOOP_VERSION
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/local/bin/python 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 실행에 필요한 최소한의 시스템 패키지 설치 (curl 추가 - healthcheck용)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    tini \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 이미 다운로드/압축해제한 Spark 폴더(JAR 포함)를 복사 (1초만에 됨)
COPY --from=builder /opt/spark /opt/spark

# Builder에서 시스템 전역에 복사한 PostgreSQL JAR도 복사
RUN mkdir -p /usr/share/java /usr/lib/jvm/java-17-openjdk-amd64/lib
COPY --from=builder /usr/share/java/postgresql-*.jar /usr/share/java/
COPY --from=builder /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-*.jar /usr/lib/jvm/java-17-openjdk-amd64/lib/

# PySpark와 Delta Lake 라이브러리를 설치한다. (이것도 캐시 최적화 적용)
# spark-base 폴더에 이 파일이 있어야 함
COPY spark-base/requirements-spark.txt . 
RUN pip install --no-cache-dir -r requirements-spark.txt

# root 권한으로 최종 실행 유저 hive를 미리 생성한다.
RUN groupadd -g 1000 hive && useradd -u 1000 -g 1000 -m hive

# root 권한으로 앞으로 hive 유저가 사용할 로그 디렉토리를 미리 만들고 소유권을 넘겨준다.
RUN mkdir -p /opt/spark/logs && chown -R hive:hive /opt/spark/logs

# Spark 실행을 위한 스크립트와 설정 파일들을 컨테이너 안으로 복사하고 실행 권한을 준다.
# 이 파일들은 자주 바뀔 수 있으므로, 가장 마지막에 복사한다.
COPY spark-base/entrypoint.sh /opt/entrypoint.sh
COPY spark-base/hive-site.xml /opt/spark/conf/
COPY spark-base/spark-defaults.conf /opt/spark/conf/

# wait-fot-it 적용
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /opt/entrypoint.sh && chmod +x /usr/local/bin/wait-for-it.sh

# 앞으로 hive 유저가 사용할 Spark 전체 디렉토리의 소유권을 넘겨준다.
# logs, work 등 모든 하위 폴더 생성 권한 문제를 한번에 해결한다.
RUN chown -R hive:hive /opt/spark

# 모든 설정이 끝난 후, 마지막에 최종 실행 유저인 hive로 전환한다.
USER hive

# 이 컨테이너가 시작될 때 실행할 기본 명령어를 설정한다.
ENTRYPOINT [ "/usr/bin/tini", "--", "/opt/entrypoint.sh" ]

