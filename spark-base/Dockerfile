# -- [Builder] - 느리고 변하지 않는 작업을 여기서 수행 -- 
# 1. Python 3.10 slim 버전으로 시작한다.
FROM python:3.10-slim-bookworm AS builder

# 2. 작성자 정보를 남긴다 
LABEL maintainer="Juseong"

# 3. 필요한 환경 변수들을 미리 설정한다.
ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG POSTGRESQL_JDBC_VERSION
# JAR 파일들을 Azure Blob Storage에서 ZIP으로 다운로드 (빠른 빌드를 위해)
ARG SPARK_TGZ_URL
ARG COMMON_JARS_ZIP_URL
ARG SPARK_JARS_ZIP_URL

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
    unzip \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 5. Spark 3.4.3 버전을 다운로드하고, 압축을 풀고, 정리한다. (Azure Blob 에서 다운받는다)
RUN curl -L -o spark.tgz "${SPARK_TGZ_URL}" \
    && tar -xzf spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark.tgz

# 6. 공통 JAR 파일들을 Azure Blob에서 다운로드
RUN curl -L -o /tmp/common-jars.zip "${COMMON_JARS_ZIP_URL}" && \
    mkdir -p ${SPARK_HOME}/jars && \
    unzip -oqj /tmp/common-jars.zip "*.jar" -d ${SPARK_HOME}/jars/ && \
    rm /tmp/common-jars.zip

# 7. Spark 전용 JAR 파일들 다운로드 (Prometheus 모니터링용)
RUN curl -L -o /tmp/spark-jars.zip "${SPARK_JARS_ZIP_URL}" && \
    unzip -oqj /tmp/spark-jars.zip "*.jar" -d ${SPARK_HOME}/jars/ && \
    rm /tmp/spark-jars.zip

# 8. BoneCP용 PostgreSQL JAR를 시스템 전역에 복사 (Java 17 호환)
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

# 9. PySpark와 Delta Lake 라이브러리 설치
COPY spark-base/requirements-spark.txt . 
RUN pip install --no-cache-dir -r requirements-spark.txt

# 10. hive 유저 생성 및 권한 설정
RUN groupadd -g 1000 hive && useradd -u 1000 -g 1000 -m hive && \
    mkdir -p /opt/spark/logs && \
    chown -R hive:hive /opt/spark

# 11. 설정 파일 및 스크립트 복사
COPY spark-base/entrypoint.sh /opt/entrypoint.sh
COPY spark-base/hive-site.xml /opt/spark/conf/
COPY spark-base/spark-defaults.conf /opt/spark/conf/
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /opt/entrypoint.sh && chmod +x /usr/local/bin/wait-for-it.sh

# 12. hive 유저로 전환
USER hive

# 이 컨테이너가 시작될 때 실행할 기본 명령어를 설정한다.
ENTRYPOINT [ "/usr/bin/tini", "--", "/opt/entrypoint.sh" ]

