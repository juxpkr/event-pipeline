# Superset specific config
import os

# PostgreSQL 연결 설정
SQLALCHEMY_DATABASE_URI = "postgresql://airflow:airflow@postgres:5432/superset"

# 보안 설정
SECRET_KEY = "5dPlRGGgNeBsiR/msDsrBwrrEe8eretPUTCVcQXlpFU="

# 포트번호
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 60

# 캐시 설정
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
}

# CSRF 설정
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# pool 설정
SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 300,
}
