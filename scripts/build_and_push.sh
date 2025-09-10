#!/bin/bash

# 스크립트 실행 중 오류가 발생하면 즉시 중단
set -e

# .env 파일이 있다면, 환경변수를 이 스크립트로 불러오기
if [ -f .env ]; then
  export $(cat .env | sed 's/#.*//g' | xargs)
fi

DOCKER_HUB_ID="juxpkr" 
VERSION="0.1"             

# --- 빌드 서비스 목록 ---

# 형식: "서비스이름:Dockerfile경로"
SERVICES_TO_BUILD=(
    "kafka:."
    "spark-base:."
    "hive:."
    "airflow:."
    "jupyter-lab:." 
    "dbt:."
    "spark-custom-exporter:."
)

# --- 자동 빌드 및 푸시 루프 ---
echo "Starting build and push process for version ${VERSION}..."

for service_info in "${SERVICES_TO_BUILD[@]}"
do
    # 서비스 이름과 Dockerfile 경로 분리
    IFS=':' read -r SERVICE_NAME DOCKER_CONTEXT <<< "$service_info"
    
    IMAGE_NAME="${DOCKER_HUB_ID}/geoevent-${SERVICE_NAME}:${VERSION}"
    
    echo "--------------------------------------------------"
    echo "Building image for ${SERVICE_NAME}..."
    echo "--------------------------------------------------"
    
    # Docker 이미지 빌드
    # Dockerfile에 필요한 변수들을 --build-arg로 직접 전달
    # 서비스별 Dockerfile 경로 설정
    if [[ "$SERVICE_NAME" == "kafka" ]]; then
        DOCKERFILE_PATH="./kafka/Dockerfile"
    elif [[ "$SERVICE_NAME" == "spark-base" ]]; then
        DOCKERFILE_PATH="./spark-base/Dockerfile"
    elif [[ "$SERVICE_NAME" == "hive" ]]; then
        DOCKERFILE_PATH="./hive/Dockerfile"
    elif [[ "$SERVICE_NAME" == "airflow" ]]; then
        DOCKERFILE_PATH="./airflow/Dockerfile"
    elif [[ "$SERVICE_NAME" == "jupyter-lab" ]]; then
        DOCKERFILE_PATH="./Dockerfile"
    elif [[ "$SERVICE_NAME" == "dbt" ]]; then
        DOCKERFILE_PATH="./transforms/Dockerfile"
    elif [[ "$SERVICE_NAME" == "spark-custom-exporter" ]]; then
        DOCKERFILE_PATH="./spark-exporter/Dockerfile"
    fi

    docker build \
      -f ${DOCKERFILE_PATH} \
      --build-arg KAFKA_IMAGE=${KAFKA_IMAGE} \
      --build-arg JMX_PROMETHEUS_JAVAAGENT_VERSION=${JMX_PROMETHEUS_JAVAAGENT_VERSION} \
      --build-arg SPARK_VERSION=${SPARK_VERSION} \
      --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
      --build-arg SPARK_TGZ_URL=${SPARK_TGZ_URL} \
      --build-arg DELTA_SPARK_VERSION=${DELTA_SPARK_VERSION} \
      --build-arg HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} \
      --build-arg AWS_SDK_VERSION=${AWS_SDK_VERSION} \
      --build-arg POSTGRESQL_JDBC_VERSION=${POSTGRESQL_JDBC_VERSION} \
      --build-arg KAFKA_CLIENTS_VERSION=${KAFKA_CLIENTS_VERSION} \
      --build-arg COMMON_JARS_ZIP_URL=${COMMON_JARS_ZIP_URL} \
      --build-arg SPARK_JARS_ZIP_URL=${SPARK_JARS_ZIP_URL} \
      --build-arg HIVE_JARS_ZIP_URL=${HIVE_JARS_ZIP_URL} \
      -t ${IMAGE_NAME} ${DOCKER_CONTEXT}
    
    echo "Pushing image ${IMAGE_NAME} to Docker Hub..."
    
    # Docker Hub에 푸시
    docker push ${IMAGE_NAME}
    
    echo "${SERVICE_NAME} image pushed successfully!"
done

echo "=================================================="
echo "All custom images have been built and pushed successfully!"
echo "=================================================="