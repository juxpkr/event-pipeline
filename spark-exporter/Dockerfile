# 1. 가벼운 파이썬 이미지를 기반으로 시작한다.
FROM python:3.10-slim-bookworm

# 2. 작업 디렉토리를 만든다.
WORKDIR /app

# 디버깅 및 healthcheck을 위한 기본 도구 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. 필요한 부품(라이브러리) 목록을 먼저 복사한다.
COPY requirements.txt .

# 4. 부품들을 설치한다.
RUN pip install --no-cache-dir -r requirements.txt

# 5. 나머지 코드 전체를 복사한다.
COPY . .

COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# 6. 컨테이너가 시작될 때, main.py를 실행
CMD ["python", "main.py"]