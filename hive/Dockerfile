# 1. apache/hive:3.1.3 이미지를 베이스로 사용한다.
FROM apache/hive:3.1.3

# 2. 필요한 JAR 파일 버전을 기본값으로 설정한다.
ARG POSTGRESQL_JDBC_VERSION=42.7.3
ARG HADOOP_AWS_VERSION=3.2.0
ARG AWS_SDK_VERSION=1.11.375

# 3. root 권한으로 필요한 도구를 설치하고, 정확한 버전의 JAR 파일들을 다운로드한다.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl net-tools procps && \
    curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRESQL_JDBC_VERSION}.jar -o /opt/hive/lib/postgresql-jdbc.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o /opt/hive/lib/hadoop-aws.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o /opt/hive/lib/aws-java-sdk-bundle.jar && \
    apt-get purge -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. Hive 설정 파일 복사
COPY hive/hive-site.xml /opt/hive/conf/
# Hadoop 핵심 설정 파일 복사
COPY hive/core-site.xml /opt/hadoop/etc/hadoop/
COPY hive/entrypoint.sh /custom-entrypoint.sh

# wait-fot-it 적용
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /custom-entrypoint.sh && chmod +x /usr/local/bin/wait-for-it.sh

# 5. 보안을 위해, 다시 원래의 'hive' 유저로 돌아간다.
USER hive