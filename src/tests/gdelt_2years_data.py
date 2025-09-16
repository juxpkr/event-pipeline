"""
GDELT 3-Way Raw Data Producer
Events, Mentions, GKG 데이터를 각각 수집하여 Kafka로 전송
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
from typing import List, Dict, Tuple

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# .env 파일에서 설정값 로드
load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 3가지 데이터 타입별 토픽
KAFKA_TOPICS = {
    "events": os.getenv("KAFKA_TOPIC_GDELT_EVENTS", "gdelt_events_raw"),
    "mentions": os.getenv("KAFKA_TOPIC_GDELT_MENTIONS", "gdelt_mentions_raw"),
    "gkg": os.getenv("KAFKA_TOPIC_GDELT_GKG", "gdelt_gkg_raw"),
}

# GDELT 데이터 타입별 파일 확장자
FILE_EXTENSIONS = {
    "events": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}


def get_historical_gdelt_urls(
    hours_to_fetch: int = 0.25, data_types: List[str] = ["events", "mentions", "gkg"]
) -> Dict[str, List[str]]:
    """
    지정한 시간만큼 과거의 GDELT 3가지 데이터 타입 URL 목록을 생성

    Args:
        hours_to_fetch: 수집할 시간 범위 (시간)
        data_types: 수집할 데이터 타입 ['events', 'mentions', 'gkg']

    Returns:
        Dict[str, List[str]]: 데이터 타입별 URL 리스트
    """
    logger.info(
        f"🔍 Generating GDELT URLs for {data_types} (last {hours_to_fetch} hours)"
    )

    # GDELT는 UTC 시간을 기준으로 함
    current_utc_time = datetime.now(timezone.utc)

    # 15분 간격으로 총 몇 번을 반복?
    num_intervals = int(hours_to_fetch * 4)

    base_url = "http://data.gdeltproject.org/gdeltv2/"
    gdelt_urls = {data_type: [] for data_type in data_types}

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

        # 각 데이터 타입별 URL 생성
        for data_type in data_types:
            if data_type not in FILE_EXTENSIONS:
                logger.warning(f"⚠️ Unknown data type: {data_type}")
                continue

            file_name = f"{timestamp_str}{FILE_EXTENSIONS[data_type]}"
            download_url = f"{base_url}{file_name}"

            if download_url not in gdelt_urls[data_type]:
                gdelt_urls[data_type].append(download_url)

    # 결과 로그
    for data_type in data_types:
        logger.info(
            f"✅ Generated {len(gdelt_urls[data_type])} URLs for {data_type.upper()}"
        )

    return gdelt_urls


def send_raw_data_to_kafka(url: str, data_type: str, producer: KafkaProducer) -> int:
    """
    URL에서 GDELT 데이터를 다운로드하고, 데이터 타입별 토픽으로 전송

    Args:
        url: GDELT 데이터 다운로드 URL
        data_type: 데이터 타입 ('events', 'mentions', 'gkg')
        producer: Kafka Producer 인스턴스

    Returns:
        int: 전송된 레코드 수
    """
    try:
        logger.info(f"📥 Downloading {data_type.upper()} data from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        topic = KAFKA_TOPICS[data_type]

        # 메모리 상에서 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            logger.info(f"📄 Processing {data_type.upper()} file: {csv_filename}")

            with z.open(csv_filename) as c:
                # CSV 파일을 한 줄씩 읽어서 처리
                # GDELT CSV는 탭으로 구분되어 있고, 헤더가 없음
                reader = csv.reader(io.TextIOWrapper(c, "utf-8"), delimiter="\t")

                record_count = 0
                batch_records = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # Raw 데이터에 메타데이터 추가
                        raw_record = {
                            "data_type": data_type,
                            "raw_data": row,  # 전체 컬럼을 리스트로
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
                                producer.send(topic, record)
                            batch_records = []

                        # 진행상황 로그 (1000개마다)
                        if record_count % 1000 == 0:
                            logger.info(
                                f"📤 Sent {record_count} {data_type.upper()} records..."
                            )

                    except Exception as e:
                        logger.warning(
                            f"⚠️ Error processing {data_type} row {row_num}: {e}"
                        )
                        continue

                # 남은 배치 전송
                if batch_records:
                    for record in batch_records:
                        producer.send(topic, record)

        producer.flush()
        logger.info(
            f"🎉 Successfully sent {record_count} {data_type.upper()} records to {topic}"
        )

        return record_count

    except Exception as e:
        logger.error(
            f"❌ Error processing {data_type} data from {url}: {e}", exc_info=True
        )
        return 0


def process_data_type(data_type: str, urls: List[str], producer: KafkaProducer) -> int:
    """
    events, mentions, gkg 데이터의 모든 URL을 처리

    Args:
        data_type: 데이터 타입
        urls: 처리할 URL 리스트
        producer: Kafka Producer

    Returns:
        int: 총 처리된 레코드 수
    """
    total_processed = 0
    logger.info(f"🚀 Starting {data_type.upper()} data processing ({len(urls)} files)")

    for i, url in enumerate(urls, 1):
        logger.info(f"🔄 Processing {data_type} file {i}/{len(urls)}")

        try:
            record_count = send_raw_data_to_kafka(url, data_type, producer)
            total_processed += record_count
            logger.info(f"✅ {data_type} file {i} completed: {record_count:,} records")

        except Exception as e:
            logger.warning(f"⚠️ Failed to process {data_type} file {url}: {e}")
            continue

        # 진행상황 로그 (5개 파일마다)
        if i % 5 == 0:
            logger.info(
                f"📈 {data_type.upper()} Progress: {i}/{len(urls)} files, {total_processed:,} records"
            )

    logger.info(
        f"🎯 {data_type.upper()} completed: {total_processed:,} records from {len(urls)} files"
    )
    return total_processed


def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting GDELT 3-Way Raw Data Producer...")

    producer = None
    total_stats = {}

    try:
        # Kafka Producer 생성
        producer = get_kafka_producer()
        logger.info("✅ Kafka producer created successfully")

        # 수집할 데이터 타입 및 시간 범위 설정
        data_types = ["events", "mentions", "gkg"]
        hours_to_fetch = 0.25

        # URL 목록 생성
        gdelt_urls = get_historical_gdelt_urls(hours_to_fetch, data_types)

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"🎯 Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in gdelt_urls or not gdelt_urls[data_type]:
                logger.warning(f"⚠️ No URLs found for {data_type}")
                continue

            processed_count = process_data_type(
                data_type, gdelt_urls[data_type], producer
            )
            total_stats[data_type] = processed_count

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info("🏁 GDELT 3-Way Data Collection Summary:")
        logger.info(f"{'='*50}")

        grand_total = 0
        for data_type, count in total_stats.items():
            logger.info(
                f"📊 {data_type.upper()}: {count:,} records → {KAFKA_TOPICS[data_type]}"
            )
            grand_total += count

        logger.info(f"🎉 GRAND TOTAL: {grand_total:,} records collected!")

    except Exception as e:
        logger.error(f"❌ Failed to initialize producer: {e}", exc_info=True)

    finally:
        if producer:
            producer.close()
            logger.info("✅ Producer closed successfully")


if __name__ == "__main__":
    main()
