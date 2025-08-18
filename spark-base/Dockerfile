# 1. Python 3.10 slim 버전으로 시작한다.
FROM python:3.10-slim-bookworm

# 2. 작성자 정보를 남긴다 
LABEL maintainer="Juseong"

# 3. 필요한 환경 변수들을 미리 설정한다.
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# PySpark가 Worker에서 Driver와 동일한 Python을 사용하도록 경로를 명시
ENV PYSPARK_PYTHON=/usr/local/bin/python 

# 4. 필요한 기본 도구와 Java를 설치한다. (-y 옵션은 모든 질문에 yes로 답하라는 뜻)
#    --no-install-recommends는 불필요한 추천 패키지를 설치하지 않아 용량을 줄여준다.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    tini \
    procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 5. Spark 3.4.3 버전을 다운로드하고, 압축을 풀고, 정리한다.
#    (내장된 Python 버전과 상관없이 Python 3.10 환경 위에 설치됨)
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# 6. PySpark와 Delta Lake 라이브러리를 설치한다.(jupyter, notebook 등 jupyter-lab에 필요한 것까지 여기서 설치)
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==2.4.0

# S3 및 Delta Lake 통신에 필요한 모든 JAR 파일을 중앙에서 관리
RUN curl -o ${SPARK_HOME}/jars/delta-core_2.12-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -o ${SPARK_HOME}/jars/delta-storage-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    curl -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# 7. Spark 실행을 위한 스크립트를 컨테이너 안으로 복사하고 실행 권한을 준다.
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# 8. 이 컨테이너가 시작될 때 실행할 기본 명령어를 설정한다.
ENTRYPOINT [ "/usr/bin/tini", "--", "/opt/entrypoint.sh" ]