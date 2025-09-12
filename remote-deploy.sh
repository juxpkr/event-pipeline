#!/bin/bash
set -e # 중간에 오류가 나면 즉시 중단

# --- 설정 (네 환경에 맞게 수정) ---
MANAGER_HOST="geoevent-manager-01" # <--- IP 대신 별명
WORKER_HOSTS=("geoevent-worker-01" "geoevent-worker-02") # <--- IP 대신 별명
REMOTE_PROJECT_PATH="/app/event-pipeline"
BRANCH="develop-vm"
# ---------------------------------

# 0. 커밋 메시지 입력받기
echo "Enter commit message (or press Enter to use a default): "
read COMMIT_MESSAGE
if [ -z "$COMMIT_MESSAGE" ]; then
    COMMIT_MESSAGE="Deploy: $(date +'%Y-%m-%d %H:%M:%S')"
fi

# 1. 로컬에서 코드 수정 후 Git에 푸시
echo ""
echo ">>>>> 1. Pushing local changes to GitHub..."
git add .
git commit -m "$COMMIT_MESSAGE"
git push origin $BRANCH

# 2. 모든 VM에 SSH로 접속해서 코드 동기화 및 권한 설정
ALL_IPS=("$MANAGER_IP" "${WORKER_IPS[@]}")
for IP in "${ALL_IPS[@]}"; do
  echo ""
  echo ">>>>> 2. Syncing code and permissions on ${IP}..."
  # git reset으로 코드 받고, 바로 이어서 chown.sh 실행
  ssh ${VM_USER}@${IP} "cd ${REMOTE_PROJECT_PATH} && git reset --hard origin/${BRANCH} && sudo ./chown.sh"
done

# 3. 매니저 노드에서만 최종 배포 실행
echo ""
echo ">>>>> 3. Deploying stack from manager node..."
ssh ${VM_USER}@${MANAGER_IP} "cd ${REMOTE_PROJECT_PATH} && sudo ./deploy.sh"

echo ""
echo "🎉 All Done!"