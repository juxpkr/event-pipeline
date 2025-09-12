#!/bin/bash

# 스크립트 실행 중 오류가 발생하면 즉시 중단
set -e

echo "✅ Loading environment variables from .env file..."

# .env 파일의 변수들을 현재 셸에 로드
export $(grep -v '^#' .env | xargs)

echo "🚀 Deploying the 'geoevent' stack to Swarm..."

# 스택 배포 실행
docker stack deploy -c docker-compose.yaml geoevent
echo "🎉 Deployment command issued successfully!"
echo "🔎 Run 'watch docker stack services geoevent' to check the status."