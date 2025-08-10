# 어떤 기본 이미지를 사용할지 명시
FROM apache/airflow:2.9.2


# 로컬의 requirements.txt 파일을 컨테이너의 /app 디렉터리로 복사
COPY requirements.txt /requirements.txt

# 복사한 파일을 바탕으로 라이브러리 설치
RUN pip install --no-cache-dir -r /requirements.txt

# 로컬의 모든 소스 코드(producer.py)를 컨테이너의 /app 디렉터리로 복사
COPY . .