#!/bin/bash

# ==============================================================================
# 모든 커스텀 Docker 이미지를 서비스별 필요 인자에 맞춰 빌드하고 Docker Hub에 푸시
# ==============================================================================

# 스크립트 실행 중 오류 발생 시 즉시 중단
set -e

# .env 파일 로드하여 모든 변수를 환경 변수로 등록
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# --- 1. 기본 설정 ---
DOCKER_HUB_ID="juxpkr"
VERSION=${1:-"0.1"}

# --- 2. 빌드할 서비스 폴더 목록 ---
SERVICES_TO_BUILD=(
    "kafka"
    "kafka-setup"
    "spark-base"
    "hive"
    "airflow"
    "transforms" 
    "spark-exporter"
)

echo "🚀 Starting build and push process for version [${VERSION}]..."
echo "=================================================="

# --- 3. 각 서비스 폴더를 순회하며 빌드 및 푸시 ---
for SERVICE_DIR in "${SERVICES_TO_BUILD[@]}"
do
    if [[ "$SERVICE_DIR" == "transforms" ]]; then
        SERVICE_NAME="dbt"
    else
        SERVICE_NAME=${SERVICE_DIR}
    fi
    
    IMAGE_NAME="${DOCKER_HUB_ID}/geoevent-${SERVICE_NAME}:${VERSION}"
    BUILD_CONTEXT="./${SERVICE_DIR}"

    echo "--------------------------------------------------"
    echo "🏭 Building: ${IMAGE_NAME}"
    echo "   Context: ${BUILD_CONTEXT}"
    echo "--------------------------------------------------"
    
    # 서비스별로 Dockerfile이 요구하는 build-arg를 다르게 주입
    case "$SERVICE_NAME" in
        kafka)
            docker build \
              --no-cache \
              --build-arg KAFKA_IMAGE="${KAFKA_IMAGE}" \
              --build-arg JMX_PROMETHEUS_JAVAAGENT_VERSION="${JMX_PROMETHEUS_JAVAAGENT_VERSION}" \
              -t ${IMAGE_NAME} ${BUILD_CONTEXT}
            ;;
        
        spark-base)
            docker build \
              --no-cache \
              --build-arg SPARK_VERSION="${SPARK_VERSION}" \
              --build-arg HADOOP_VERSION="${HADOOP_VERSION}" \
              --build-arg POSTGRESQL_JDBC_VERSION="${POSTGRESQL_JDBC_VERSION}" \
              --build-arg SPARK_TGZ_URL="${SPARK_TGZ_URL}" \
              --build-arg COMMON_JARS_ZIP_URL="${COMMON_JARS_ZIP_URL}" \
              --build-arg SPARK_JARS_ZIP_URL="${SPARK_JARS_ZIP_URL}" \
              -f ${BUILD_CONTEXT}/Dockerfile -t ${IMAGE_NAME} .
            ;;

        hive)
            docker build \
              --no-cache \
              --build-arg POSTGRESQL_JDBC_VERSION="${POSTGRESQL_JDBC_VERSION}" \
              --build-arg HIVE_JARS_ZIP_URL="${HIVE_JARS_ZIP_URL}" \
              -f ${BUILD_CONTEXT}/Dockerfile -t ${IMAGE_NAME} .
            ;;

        airflow)
            docker build \
              --no-cache \
              --build-arg SPARK_TGZ_URL="${SPARK_TGZ_URL}" \
              --build-arg SPARK_VERSION="${SPARK_VERSION}" \
              --build-arg HADOOP_VERSION="${HADOOP_VERSION}" \
              --build-arg COMMON_JARS_ZIP_URL="${COMMON_JARS_ZIP_URL}" \
              --build-arg DELTA_SPARK_VERSION="${DELTA_SPARK_VERSION}" \
              --build-arg HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION}" \
              --build-arg AWS_SDK_VERSION="${AWS_SDK_VERSION}" \
              --build-arg POSTGRESQL_JDBC_VERSION="${POSTGRESQL_JDBC_VERSION}" \
              --build-arg KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION}" \
              -f ${BUILD_CONTEXT}/Dockerfile -t ${IMAGE_NAME} .
            ;;

        *)
            # kafka-setup, dbt, spark-exporter 등 ARG가 없는 나머지 서비스들
            docker build -t ${IMAGE_NAME} ${BUILD_CONTEXT}
            ;;
    esac
    
    echo "--------------------------------------------------"
    echo "📤 Pushing: ${IMAGE_NAME}"
    echo "--------------------------------------------------"
    docker push ${IMAGE_NAME}
    echo "✅ Push successful for ${SERVICE_NAME}"
    echo ""
done

# --- 4. 루트 Dockerfile을 사용하는 jupyter-lab 별도 처리 ---
echo "--------------------------------------------------"
echo "🏭 Building special case: jupyter-lab"
echo "--------------------------------------------------"
JUPYTER_IMAGE_NAME="${DOCKER_HUB_ID}/geoevent-jupyter-lab:${VERSION}"
docker build --no-cache -t ${JUPYTER_IMAGE_NAME} -f ./Dockerfile .

echo "--------------------------------------------------"
echo "📤 Pushing: ${JUPYTER_IMAGE_NAME}"
echo "--------------------------------------------------"
docker push ${JUPYTER_IMAGE_NAME}
echo "✅ Push successful for jupyter-lab"
echo ""

# --- 5. 최종 완료 ---
echo "=================================================="
echo "🎉 All custom images have been built and pushed successfully!"
echo "   Version Tag: [${VERSION}]"
echo "=================================================="