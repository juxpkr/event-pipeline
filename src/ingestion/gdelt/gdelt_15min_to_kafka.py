"""
GDELT Raw Data Producer - ZIP 파일에서 순수 RAW 데이터를 Kafka로 전송
정제나 컬럼명 매핑 없이 순수 RAW 데이터만 전송
"""

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
from src.utils.kafka_producer import get_kafka_producer

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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GDELT", "gdelt_raw_events")  # Raw 토픽


def get_latest_gdelt_data_url():
    """GDELT의 lastupdate.txt 파일을 읽고, 최근 15분 마이크로배치 데이터의 URL을 찾아 반환"""
    try:
        latest_gdelt_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        response = requests.get(latest_gdelt_url)
        response.raise_for_status()

        # export.CSV.zip 찾기
        for line in response.text.split("\n"):
            if "export.CSV.zip" in line:
                url = line.split(" ")[2]
                logger.info(f"✅ Found latest GDELT data URL: {url}")
                return url

        logger.warning("⚠️ export.CSV.zip not found")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to fetch latest GDELT URL: {e}")
        return None


def send_raw_data_to_kafka(url: str, producer: KafkaProducer):
    """URL에서 GDELT 데이터를 다운로드하고, 순수 RAW 형태로 Kafka에 전송"""
    try:
        logger.info(f"📥 Downloading RAW data from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # 메모리 상에서 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            logger.info(f"📄 Processing RAW file: {csv_filename}")

            with z.open(csv_filename) as c:
                # CSV 파일을 한 줄씩 읽어서 처리
                # GDELT CSV는 탭으로 구분되어 있고, 헤더가 없음
                reader = csv.reader(io.TextIOWrapper(c, "utf-8"), delimiter="\t")

                record_count = 0
                batch_records = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # 순수 RAW 데이터 - 컬럼명 없이 리스트 형태로
                        raw_record = {
                            "raw_data": row,  # 전체 컬럼을 리스트로 (컬럼명 없음)
                            "row_number": row_num,
                            "source_file": csv_filename,
                            "extracted_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "source_url": url,
                            "total_columns": len(row),
                        }

                        batch_records.append(raw_record)
                        record_count += 1

                        # 배치 전송 (100개씩)
                        if len(batch_records) >= 100:
                            for record in batch_records:
                                producer.send(KAFKA_TOPIC, record)
                            batch_records = []

                        # 진행상황 로그 (1000개마다)
                        if record_count % 1000 == 0:
                            logger.info(f"📤 Sent {record_count} RAW records...")

                    except Exception as e:
                        logger.warning(f"⚠️ Error processing row {row_num}: {e}")
                        continue

                # 남은 배치 전송
                if batch_records:
                    for record in batch_records:
                        producer.send(KAFKA_TOPIC, record)

        producer.flush()
        logger.info(
            f"🎉 Successfully sent {record_count} RAW records from {csv_filename}"
        )
        logger.info(f"📤 RAW data sent to Kafka topic: '{KAFKA_TOPIC}'")

        return record_count

    except Exception as e:
        logger.error(f"❌ Error during RAW data processing: {e}", exc_info=True)
        return 0


def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting GDELT Raw Data Producer...")

    producer = None  # finally에서 producer 변수를 인식하도록 미리 선언

    try:
        # Kafka Producer 유틸을 사용해서 생성
        producer = get_kafka_producer()

        latest_url = get_latest_gdelt_data_url()
        if latest_url:
            send_raw_data_to_kafka(latest_url, producer)
        else:
            logger.error("❌ Could not get latest GDELT URL")

    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("✅ Raw Producer closed successfully")


if __name__ == "__main__":
    main()
