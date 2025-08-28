import os
import requests
import zipfile
import io
import csv
import json
import time
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# .env 파일에서 설정값 로드
load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GDELT", "gdelt_events")


def get_latest_gdelt_data_url():
    # GDELT의 lastupdate.txt 파일을 읽고, 최근 15분 마이크로배치 데이터의 url을 찾아 반환한다.

    try:
        latest_gdelt_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        response = requests.get(latest_gdelt_url)
        response.raise_for_status()

        # export.CSV.zip 찾기
        for line in response.text.split("\n"):
            if "export.CSV.zip" in line:
                # 그 줄을 공백으로 쪼개고, 세번째 URL 부분만 반환
                url = line.split(" ")[2]
                logger.info(f"Found latest GDELT data URL: {url}")
                return url
        # for문이 끝났는데 못 찾았으면, None 반환
        logger.warning("Not found export.CSV.zip")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch latest GDELT URL: {e}")
        return None


def process_zip_stream_to_kafka(url: str, producer: KafkaProducer):
    # URL에서 GDELT 데이터를 다운로드하고, 압축을 푼 뒤 Kafka로 전송한다.
    try:
        logger.info(f"Downloading data from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # 메모리 상에서 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # 압축 파일 안의 첫 번째 파일(CSV) 이름을 가져옴
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as c:
                # CSV 파일을 한 줄씩 읽어서 처리
                # GDELT CSV는 탭으로 구분되어 있고, 헤더가 없음
                reader = csv.reader(io.TextIOWrapper(c, "utf-8"), delimiter="\t")

                # GDELT 2.0 이벤트 데이터베이스의 컬럼명 (필요한 것만 선택 가능)
                headers = [
                    "GlobalEventID",
                    "Day",
                    "MonthYear",
                    "Year",
                    "FractionDate",
                    "Actor1Code",
                    "Actor1Name",
                    "Actor1CountryCode",
                    "Actor1KnownGroupCode",
                    "Actor1EthnicCode",
                    "AvgTone",
                    "EventRootCode",
                    "QuadClass",
                ]

                for row in reader:
                    # 각 행을 딕셔너리로 변환 (헤더와 값 매핑)
                    # 전체 컬럼을 다 쓸 경우
                    record = {header: value for header, value in zip(headers, row)}

                    # JSON으로 변환하여 Kafka로 전송
                    producer.send(KAFKA_TOPIC, record)

        producer.flush()
        logger.info(
            f"Successfully processed and sent data from {csv_filename} to Kafka topic '{KAFKA_TOPIC}'."
        )

    except Exception as e:
        logger.error(
            f"An error occurred during data fetching or processing: {e}", exc_info=True
        )


def main():
    """메인 실행 함수"""
    logger.info("Starting GDELT Microbatch Producer...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return

    while True:
        latest_url = get_latest_gdelt_data_url()
        if latest_url:
            process_zip_stream_to_kafka(latest_url, producer)

        # 15분마다 실행
        # logger.info("Waiting for 15 minutes before the next run...")
        # time.sleep(900)


if __name__ == "__main__":
    main()
