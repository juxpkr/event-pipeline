# 1. Spark 및 Airflow와 동일한 Python 3.10-slim 버전으로 시작한다.
FROM python:3.10-slim-bookworm

# 2. 작업 디렉토리를 /app으로 설정한다. (앞으로 모든 명령어는 이 폴더 안에서 실행됨)
WORKDIR /app

# 3. Spark Driver 실행에 필요한 Java와 ps 명령어를 설치한다.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    build-essential \
    libsasl2-dev \
    procps \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. Java 환경 변수를 설정한다.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 5. requirements.txt 파일을 먼저 복사해서 라이브러리부터 설치한다.
#    (이렇게 하면, 코드만 바뀔 경우 라이브러리를 매번 새로 설치하는 과정을 건너뛸 수 있어 빌드 속도가 빨라짐)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# JAR 파일 디렉토리 생성 (Volume으로 spark-master와 공유)
RUN mkdir -p /opt/spark/jars

# 6. 현재 디렉토리의 모든 파일들을 컨테이너의 작업 디렉토리(/app)로 복사한다.
COPY . .

# 7. 모든 설치를 끝내고, 보안을 위해 권한 없는 사용자로 전환한다.
#USER nobody

# 8. Jupyter Lab이 사용하는 포트(8888)를 외부에 노출시킨다.
EXPOSE 8888

# 9. 컨테이너가 시작될 때 실행할 명령어를 설정한다.
#    --ip=0.0.0.0 : 컨테이너 외부에서의 접속을 허용
#    --port=8888 : 8888 포트에서 실행
#    --no-browser : 컨테이너 안에서 브라우저를 자동으로 열지 않음
#    --allow-root : root 계정으로 실행 허용
#    --ServerApp.token='' : 로그인 시 토큰 암호 없이 접속 허용 (개발용)
#    --ServerApp.password='' : 로그인 시 패스워드 없이 접속 허용 (개발용)
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''"]
