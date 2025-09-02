# 1. Apache의 Python 3.10 기반 공식 이미지를 기본 모델로 사용
FROM apache/airflow:2.9.2-python3.10

# 2. root 사용자로 전환해서 모든 설치/빌드 작업을 수행
USER root

# 3. 시스템 패키지 및 Java 설치 (Spark 실행을 위해 JDK 추가)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
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

# 6. Spark 다운로드 및 설치
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
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

# 8. Spark 설정 파일 복사
COPY ./spark-base/spark-defaults.conf /opt/spark/conf/
COPY ./spark-base/hive-site.xml /opt/spark/conf/

# 9. pip install을 실행하기 전에, 반드시 airflow 사용자로 돌아와야 함
USER airflow

# 9. Airflow에 필요한 Python 라이브러리 설치
COPY ./airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt