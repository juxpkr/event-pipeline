#!/bin/bash
set -e # 중간에 오류가 나면 즉시 중단

# --- 설정 ---
BRANCH="develop-vm"
CONTROL_VM_HOST="control-vm" # control-vm의 SSH 별명
REMOTE_PROJECT_PATH="/app/event-pipeline"
# ------------

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
echo ">>>>> Push complete."

# 2. Control VM에 접속해서, 원격으로 '그곳에 있는' remote-deploy.sh를 실행
echo ""
echo ">>>>> 2. Remotely executing deploy script on ${CONTROL_VM_HOST}..."
ssh ${CONTROL_VM_HOST} "cd ${REMOTE_PROJECT_PATH} && ./remote-deploy.sh"

echo ""
echo "🎉 All Done! Master deployment from local PC is complete."