#!/bin/bash
set -e # 중간에 오류가 나면 즉시 중단

# --- 설정 (네 환경에 맞게 수정) ---
MANAGER_HOST="geoevent-manager-01" # <--- IP 대신 별명
WORKER_HOSTS=("geoevent-worker-01" "geoevent-worker-02") # <--- IP 대신 별명
MONITORING_HOSTS=("geoevent-monitoring-01") # <--- IP 대신 별명  
REMOTE_PROJECT_PATH="/app/event-pipeline"
BRANCH="develop-vm-final"
# ---------------------------------

## 0. 커밋 메시지 입력받기
#echo "Enter commit message (or press Enter to use a default): "
#read COMMIT_MESSAGE
#if [ -z "$COMMIT_MESSAGE" ]; then
#    COMMIT_MESSAGE="Deploy: $(date +'%Y-%m-%d %H:%M:%S')"
#fi
#
## 1. 로컬에서 코드 수정 후 Git에 푸시
#echo ""
#echo ">>>>> 1. Pushing local changes to GitHub..."
#git add .
#git commit -m "$COMMIT_MESSAGE"
#git push origin $BRANCH

# GitHub 전파 지연을 위한 5초 대기
echo ">>>>> Waiting 5 seconds for GitHub propagation..."
sleep 5

# 2. 모든 VM에 SSH로 접속해서 코드 동기화 및 권한 설정
ALL_HOSTS=("$MANAGER_HOST" "${WORKER_HOSTS[@]}" "${MONITORING_HOSTS[@]}")
for HOST in "${ALL_HOSTS[@]}"; do
  echo ""
  echo ">>>>> DEBUG: Current HOST variable is [${HOST}] <<<<<"
  # --------------------
  echo ">>>>> 2. Syncing code and permissions on ${HOST}..."
  ssh ${HOST} "cd ${REMOTE_PROJECT_PATH} && sudo git fetch origin && sudo git reset --hard origin/${BRANCH} && sudo git pull origin ${BRANCH} && sudo chmod +x chown.sh && sudo ./chown.sh"
done

echo ">>>>> Waiting for 5 seconds to ensure network stabilization..."
sleep 5

ssh ${MANAGER_HOST} "cd ${REMOTE_PROJECT_PATH} && sudo chmod +x deploy.sh

## 3. 매니저 노드에서만 최종 배포 실행
#echo ""
#echo ">>>>> 3. Deploying stack from manager node..."
#ssh ${MANAGER_HOST} "cd ${REMOTE_PROJECT_PATH} && sudo chmod +x deploy.sh && sudo ./deploy.sh"
#
#echo ""
#echo "🎉 All Done!"
