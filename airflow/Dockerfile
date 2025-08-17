# 1. Airflow 제작사(Apache)의 Python 3.10 기반 공식 이미지를 기본 모델로 사용
FROM apache/airflow:2.9.2-python3.10

# 2. root 사용자로 잠시 전환해서, 시스템 패키지를 설치할 권한을 얻는다
USER root

# 3. Spark 실행에 필수적인 Java(JDK)와 ps 명령어가 포함된 procps를 설치한다
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk procps && \
    apt-get clean

# 4. Spark가 Java를 찾을 수 있도록 JAVA_HOME 환경 변수를 설정한다
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 5. 다시 원래의 airflow 사용자로 돌아와서 보안을 유지한다
USER airflow

# 6. Airflow가 우리 파이프라인을 조종하는 데 필요한 라이브러리 목록을 복사한다
COPY requirements-airflow.txt /requirements-airflow.txt

# 7. 복사한 목록을 바탕으로 라이브러리를 설치한다
RUN pip install --no-cache-dir -r /requirements-airflow.txt
