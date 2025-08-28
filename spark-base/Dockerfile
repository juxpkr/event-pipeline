# -- [Builder] - 느리고 변하지 않는 작업을 여기서 수행 -- 
# 1. Python 3.10 slim 버전으로 시작한다.
FROM python:3.10-slim-bookworm AS builder

# 2. 작성자 정보를 남긴다 
LABEL maintainer="Juseong"

# 3. 필요한 환경 변수들을 미리 설정한다.
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
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
RUN curl -o ${SPARK_HOME}/jars/delta-core_2.12-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -o ${SPARK_HOME}/jars/delta-storage-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    curl -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -o ${SPARK_HOME}/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -o ${SPARK_HOME}/jars/commons-logging-1.2.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar

# BoneCP용 PostgreSQL JAR를 시스템 전역에 복사 (Java 17 호환)
RUN mkdir -p /usr/share/java && \
    cp ${SPARK_HOME}/jars/postgresql-42.7.3.jar /usr/share/java/ && \
    cp ${SPARK_HOME}/jars/postgresql-42.7.3.jar /usr/lib/jvm/java-17-openjdk-amd64/lib/

# -- Builder에서 결과물만 빠르게 복사 -- 
FROM python:3.10-slim-bookworm

# 작성자, 환경 변수 등은 최종 이미지에 다시 한번 정의해준다.
LABEL maintainer="Juseong"
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/local/bin/python 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 실행에 필요한 최소한의 시스템 패키지만 설치 (curl은 이제 필요 없음)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    tini \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 이미 다운로드/압축해제한 Spark 폴더(JAR 포함)를 복사 (1초만에 됨)
COPY --from=builder /opt/spark /opt/spark

# Builder에서 시스템 전역에 복사한 PostgreSQL JAR도 복사
RUN mkdir -p /usr/share/java /usr/lib/jvm/java-17-openjdk-amd64/lib
COPY --from=builder /usr/share/java/postgresql-42.7.3.jar /usr/share/java/
COPY --from=builder /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.7.3.jar /usr/lib/jvm/java-17-openjdk-amd64/lib/

# PySpark와 Delta Lake 라이브러리를 설치한다. (이것도 캐시 최적화 적용)
# spark-base 폴더에 이 파일이 있어야 함
COPY requirements-spark.txt . 
RUN pip install --no-cache-dir -r requirements-spark.txt

# Spark 실행을 위한 스크립트와 설정 파일들을 컨테이너 안으로 복사하고 실행 권한을 준다.
# 이 파일들은 자주 바뀔 수 있으므로, 가장 마지막에 복사한다.
COPY entrypoint.sh /opt/entrypoint.sh
COPY hive-site.xml /opt/spark/conf/
COPY spark-defaults.conf /opt/spark/conf/
RUN chmod +x /opt/entrypoint.sh

# 이 컨테이너가 시작될 때 실행할 기본 명령어를 설정한다.
ENTRYPOINT [ "/usr/bin/tini", "--", "/opt/entrypoint.sh" ]

