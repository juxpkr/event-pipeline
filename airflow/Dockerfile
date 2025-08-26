# 1. Apache의 Python 3.10 기반 공식 이미지를 기본 모델로 사용
FROM apache/airflow:2.9.2-python3.10

# 2. root 사용자로 전환해서 모든 설치/빌드 작업을 수행
USER root

# 3. 시스템 패키지 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk procps \
    build-essential \
    libsasl2-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. airflow 유저를 staff 그룹에 추가해서 빌드 도구 사용 권한 부여
#    이걸로 sudo 없이 permission denied 문제 해결 할 수 있음
RUN usermod -aG staff airflow

# 5. Spark가 Java를 찾을 수 있도록 JAVA_HOME 환경 변수를 설정
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 6. pip install을 실행하기 전에, 반드시 airflow 사용자로 돌아와야 함
USER airflow

# 7. requirements.txt 파일만 먼저 복사
COPY requirements-airflow.txt /requirements-airflow.txt

# 8. 라이브러리를 먼저 설치해서 이 결과를 Docker 캐시에 저장
RUN pip install --no-cache-dir -r /requirements-airflow.txt

