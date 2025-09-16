# .env에서 전달받은 변수들
ARG KAFKA_IMAGE
ARG JMX_PROMETHEUS_JAVAAGENT_VERSION

# Confluent Kafka 이미지를 기반으로 커스텀 이미지를 만든다
FROM confluentinc/cp-kafka:7.5.0

# 의존성 설치 및 JMX Agent 다운로드
USER root

RUN microdnf install -y curl && \
    mkdir -p /opt/kafka/libs/ && \
    curl -L -o /opt/kafka/libs/jmx_prometheus_javaagent-1.4.0.jar \
    https://github.com/prometheus/jmx_exporter/releases/download/1.4.0/jmx_prometheus_javaagent-1.4.0.jar && \
    curl -L -o /usr/bin/tini https://github.com/krallin/tini/releases/download/v0.19.0/tini && \
    chmod +x /usr/bin/tini && \
    microdnf clean all

# 다운로드한 JAR 파일 권한 설정
RUN chmod 644 /opt/kafka/libs/jmx_prometheus_javaagent-1.4.0.jar

# 다시 기본 사용자인 appuser로 전환
USER appuser