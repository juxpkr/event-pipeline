# 1. apache/hive:3.1.3 이미지를 베이스로 사용한다.
FROM apache/hive:3.1.3

# 2. 필요한 JAR 파일 버전을 기본값으로 설정한다.
ARG POSTGRESQL_JDBC_VERSION=42.7.3
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION

# 3. root 권한으로 필요한 도구를 설치한다.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl unzip net-tools procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. 디버깅: Hadoop 2.x 호환 버전으로 직접 다운로드 테스트
RUN curl -L -o /opt/hive/lib/hadoop-aws-2.10.1.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.1/hadoop-aws-2.10.1.jar" && \
    curl -L -o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" && \
    curl -L -o /opt/hive/lib/postgresql-42.7.3.jar \
    "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

# # 4. Hive 전용 JAR 파일들을 Azure Blob에서 다운로드한다. (주석 처리)
# ARG HIVE_JARS_ZIP_URL
# RUN curl -L -o /tmp/hive-jars.zip "${HIVE_JARS_ZIP_URL}" && \
#     unzip -oqj /tmp/hive-jars.zip "*.jar" -d /opt/hive/lib/ && \
#     rm /tmp/hive-jars.zip 

# 5. Hive 설정 파일 복사
COPY hive/hive-site.xml /opt/hive/conf/
# Hadoop 핵심 설정 파일 복사
COPY hive/core-site.xml /opt/hadoop/etc/hadoop/
COPY hive/entrypoint.sh /custom-entrypoint.sh

# wait-for-it 적용
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /custom-entrypoint.sh && chmod +x /usr/local/bin/wait-for-it.sh

# 6. 보안을 위해, 다시 원래의 'hive' 유저로 돌아간다.
USER hive