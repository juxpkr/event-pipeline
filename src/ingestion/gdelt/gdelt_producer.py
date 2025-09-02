import os
import json
from google.cloud import bigquery
from kafka import KafkaProducer
from dotenv import load_dotenv


def main():

    # BigQuery에서 GDELT 데이터를 가져와 Kafka 토픽으로 전송하는 함수.

    # 1. GCP 인증 설정
    # .env 파일의 절대 경로를 직접 지정
    project_root_in_container = "/opt/airflow"
    dotenv_path = os.path.join(project_root_in_container, ".env")

    # 지정된 경로의 .env 파일을 로드
    load_dotenv(dotenv_path=dotenv_path)

    try:
        """
        service_account_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_key_path and os.path.exists(service_account_key_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path
            print(f"GCP 서비스 계정 키 로드.)")
        else:
            print(".env 파일에 키 없음")
        """
        # Airflow 컨테이너 내부의 프로젝트 루트는 '/opt/airflow'
        project_root_in_container = "/opt/airflow"

        # .env에서 상대 경로를 읽어옴
        keyfile_relative_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if keyfile_relative_path:
            # 컨테이너 내부의 절대 경로를 만들어줌
            keyfile_absolute_path = os.path.join(
                project_root_in_container, keyfile_relative_path
            )

            if os.path.exists(keyfile_absolute_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_absolute_path
                print("GCP 서비스 계정 키를 성공적으로 로드했습니다.")
            else:
                print(f"경고: 파일 경로를 찾을 수 없습니다: {keyfile_absolute_path}")
                return  # 에러 발생 시 함수 종료
        else:
            print("경고: .env 파일에 GCP_KEYFILE_PATH가 없습니다.")
            return  # 에러 발생 시 함수 종료
    except Exception as e:
        print(f"GCP 인증 설정 중 에러 발생: {e}")
        return

    # 2. BigQuery 클라이언트 생성
    try:
        bq_client = bigquery.Client()
        print("BigQuery 클라이언트 생성 성공.")
    except Exception as e:
        print(f"BigQuery 클라이언트 생성 실패: {e}")
        return

    # 3. Kafka 프로듀서 생성
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:29092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Kafka 프로듀서 생성 성공.")
    except Exception as e:
        print(f"Kafka 프로듀서 생성 실패: {e}")
        return

    # 4. BigQuery에서 데이터 가져오기
    query = """
        SELECT Actor1CountryCode, COUNT(*) AS event_count
        FROM `gdelt-bq.gdeltv2.events`
        WHERE Year = 2024 AND Actor1CountryCode IS NOT NULL
        GROUP BY Actor1CountryCode
        LIMIT 20
    """
    try:
        print("BigQuery에 쿼리 실행 중...")
        query_job_result = bq_client.query(query).result()
        print(f"쿼리 완료. {query_job_result.total_rows}개의 행을 가져왔습니다.")
    except Exception as e:
        print(f"BigQuery 쿼리 실행 실패: {e}")
        return

    # 5. Kafka로 데이터 전송
    topic_name = "gdelt_events"
    try:
        print(f"Kafka 토픽 '{topic_name}'으로 데이터 전송 시작...")
        for row in query_job_result:
            message = dict(row)
            producer.send(topic_name, value=message)

        producer.flush()
        print("데이터 전송 완료.")
    except Exception as e:
        print(f"Kafka로 데이터 전송 실패: {e}")


if __name__ == "__main__":
    main()
