# 어떤 기본 이미지를 사용할지 명시
# 파이썬 코드를 실행해야 하므로, 파이썬 이미지를 가져온다.
FROM python:3.9-slim

# 컨테이너 내부에서 작업할 디렉터리를 설정
# 모든 파일은 이 /app 디렉터리 안에서 다룬다.
WORKDIR /app

# 로컬의 requirements.txt 파일을 컨테이너의 /app 디렉터리로 복사
COPY requirements.txt .

# 복사한 파일을 바탕으로 라이브러리 설치
RUN pip install --no-cache-dir -r requirements.txt

# 로컬의 모든 소스 코드(producer.py)를 컨테이너의 /app 디렉터리로 복사
COPY . .