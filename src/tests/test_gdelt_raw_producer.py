"""
GDELT Raw Data Producer - ZIP 파일에서 순수 RAW 데이터를 Kafka로 전송
최근 이틀치의 데이터 수집 - EDA용도
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
from datetime import datetime, timedelta, timezone

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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GDELT", "gdelt_raw_events_test")  # Test 토픽


def get_historical_gdelt_urls(hours_to_fetch=6):
    """지정한 시간만큼 과거의 GDELT 데이터 URL 목록을 생성"""
    logger.info(f"🔍 Generating GDELT URLs for the last {hours_to_fetch} hours...")

    # GDELT는 UTC 시간을 기준으로 함
    current_utc_time = datetime.now(timezone.utc)

    # 15분 간격으로 총 몇 번을 반복?
    # 24시간 * (60 /15) = 96번
    num_intervals = hours_to_fetch * 4

    base_url = "http://data.gdeltproject.org/gdeltv2/"
    gdelt_urls = []

    for i in range(num_intervals):
        # 현재 시각에서 15분씩 과거로 이동
        target_time = current_utc_time - timedelta(minutes=15 * i)

        # GDELT URL 형식에 맞는 타임스탬프 문자열 생성 (YYYYMMDDHHMMSS)
        # 15분 단위 깔끔하게 맞추기
        minute_rounded = (target_time.minute // 15) * 15
        target_time_rounded = target_time.replace(
            minute=minute_rounded, second=0, microsecond=0
        )

        timestamp_str = target_time_rounded.strftime("%Y%m%d%H%M%S")

        # 최종 URL 조합
        file_name = f"{timestamp_str}.export.CSV.zip"
        download_url = f"{base_url}{file_name}"

        if download_url not in gdelt_urls:
            gdelt_urls.append(download_url)

    logger.info(f"✅ Generated {len(gdelt_urls)} GDELT URLs")
    return gdelt_urls


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
    logger.info("🚀 Starting GDELT Historical Raw Data Producer...")

    producer = None
    total_processed = 0

    try:
        # Kafka Producer 생성
        producer = get_kafka_producer()
        logger.info("✅ Kafka producer created successfully")

        # 최근 24시간 URL 목록 생성
        gdelt_urls = get_historical_gdelt_urls(hours_to_fetch=6)

        # 각 URL별로 데이터 처리
        for i, url in enumerate(gdelt_urls, 1):
            logger.info(f"🔄 Processing file {i}/{len(gdelt_urls)}: {url}")

            try:
                record_count = send_raw_data_to_kafka(url, producer)
                total_processed += record_count
                logger.info(f"✅ File {i} completed: {record_count} records")

            except Exception as e:
                logger.warning(f"⚠️ Failed to process {url}: {e}")
                continue

            # 진행상황 로그 (10개 파일마다)
            if i % 10 == 0:
                logger.info(
                    f"📈 Progress: {i}/{len(gdelt_urls)} files processed, {total_processed:,} total records"
                )

        logger.info(
            f"🎯 All processing completed: {total_processed:,} total records from {len(gdelt_urls)} files"
        )

    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("✅ Raw Producer closed successfully")


if __name__ == "__main__":
    main()
