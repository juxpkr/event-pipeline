# =======================================================
# ======= The Final Permission Setup Script =======
# =======================================================

echo "🚀 Starting final permission setup..."

# --- Airflow Permissions ---
# Airflow 컨테이너(UID 50000)가 접근해야 하는 모든 폴더의 소유권을 변경
echo "Changing ownership for Airflow directories..."
sudo chown -R 50000:50000 /app/event-pipeline/dags
sudo chown -R 50000:50000 /app/event-pipeline/logs
sudo chown -R 50000:50000 /app/event-pipeline/plugins
sudo chown -R 50000:50000 /app/event-pipeline/src
sudo chown -R 50000:50000 /app/event-pipeline/config
sudo chown -R 50000:50000 /app/event-pipeline/.secrets
sudo chown -R 50000:50000 /app/event-pipeline/.env


# --- Grafana Permissions ---
# Grafana 컨테이너(UID 472)가 접근해야 하는 폴더의 소유권을 변경
echo "Changing ownership for Grafana directories..."
sudo chown -R 472:472 /app/event-pipeline/monitoring/grafana


# --- Spark Permissions ---
# Spark 서비스들은 현재 user: root 로 실행 중이므로 권한 변경이 필요 없음.
echo "Spark services are running as root, skipping chown."

# --- Superset Permissions ---
sudo chmod +x ./config/superset/*.sh

echo "✅ All permissions have been set successfully!"