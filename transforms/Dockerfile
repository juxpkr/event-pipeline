# 1. dbt 실행을 위한 Python 3.10-slim 이미지를 기본 모델로 사용
FROM python:3.10-slim-bookworm

# 2. root 사용자로 잠시 전환해서, 시스템 패키지를 설치할 권한을 얻는다
USER root

# 3. sasl 패키지 빌드에 필요한 build-essential, libsasl2-dev 등 시스템 패키지를 설치한다
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsasl2-dev \
    libldap2-dev \
    python3-dev \
    libssl-dev \
    git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. 보안을 위해 dbt 전용 사용자를 만들고, 홈 디렉터리도 함께 생성한다
RUN useradd --create-home --shell /bin/bash dbt_user

# 5. 이제부터는 dbt_user 권한으로 명령을 실행한다
USER dbt_user

# 6. 작업 디렉토리를 설정한다
WORKDIR /app

# 7. dbt 실행에 필요한 라이브러리 목록 파일을 컨테이너 안으로 복사한다
COPY requirements-dbt.txt .

# 8. 복사한 목록을 바탕으로 라이브러리를 설치한다
RUN pip install --no-cache-dir -r requirements-dbt.txt

# 9. dbt 실행 경로를 PATH에 추가
ENV PATH="/home/dbt_user/.local/bin:$PATH"
