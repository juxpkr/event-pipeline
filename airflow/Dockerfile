# 1. Apache의 Python 3.10 기반 공식 이미지를 기본 모델로 사용
FROM apache/airflow:2.9.2-python3.10

# 2. root 사용자로 전환해서 모든 설치/빌드 작업을 수행
USER root

# 3. 시스템 패키지 및 Java 설치 (Spark 실행을 위해 JDK 추가)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    unzip \
    procps \
    build-essential \
    libsasl2-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. airflow 유저를 staff 그룹에 추가해서 빌드 도구 사용 권한 부여
RUN usermod -aG staff airflow

# 5. Spark 및 Java 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ARG SPARK_VERSION
ARG HADOOP_VERSION
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/local/bin/python

# 6. Spark 다운로드 및 설치 (Azure Blob에서 빠르게 다운로드)
ARG SPARK_TGZ_URL
RUN curl -L -o /tmp/spark.tgz "${SPARK_TGZ_URL}" \
    && tar -xvzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# 7. root로 전환해서 JAR 파일 다운로드 (권한 문제 해결)
USER root

ARG DELTA_SPARK_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION
ARG POSTGRESQL_JDBC_VERSION
ARG KAFKA_CLIENTS_VERSION

# Airflow에서 Spark 연동을 위한 공통 JAR 파일들을 Azure Blob에서 다운로드
ARG COMMON_JARS_ZIP_URL
RUN curl -L -o /tmp/common-jars.zip "${COMMON_JARS_ZIP_URL}" && \
    mkdir -p ${SPARK_HOME}/jars && \
    unzip -oqj /tmp/common-jars.zip "*.jar" -d ${SPARK_HOME}/jars/ && \
    rm /tmp/common-jars.zip

# 9. Spark 설정 파일 복사
COPY spark-base/spark-defaults.conf /opt/spark/conf/
COPY spark-base/hive-site.xml /opt/spark/conf/
COPY monitoring/jmx/spark_jmx_config.yml /opt/spark/conf/jmx-config.yml

# wait-fot-it 적용
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

RUN groupadd --gid 999 docker_in_docker && usermod -aG docker_in_docker airflow

# 9. pip install을 실행하기 전에, 반드시 airflow 사용자로 돌아와야 함
USER airflow

# Spark Kafka SQL 패키지 미리 다운로드하여 캐싱
RUN spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --version

# 9. Airflow에 필요한 Python 라이브러리 설치
COPY ./airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt