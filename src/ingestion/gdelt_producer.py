"""
GDELT 3-Way Bronze Data Producer
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
from confluent_kafka import Producer
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
    "events": os.getenv("KAFKA_TOPIC_GDELT_EVENTS", "gdelt_events_bronze"),
    "mentions": os.getenv("KAFKA_TOPIC_GDELT_MENTIONS", "gdelt_mentions_bronze"),
    "gkg": os.getenv("KAFKA_TOPIC_GDELT_GKG", "gdelt_gkg_bronze"),
}

# GDELT 데이터 타입별 파일 확장자
FILE_EXTENSIONS = {
    "events": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}


def get_latest_gdelt_urls(
    data_types: List[str] = ["events", "mentions", "gkg"]
) -> Dict[str, List[str]]:
    """
    최신 GDELT 3가지 데이터 타입의 URL을 생성 (단일 15분 배치)

    Args:
        data_types: 수집할 데이터 타입 ['events', 'mentions', 'gkg']

    Returns:
        Dict[str, List[str]]: 데이터 타입별 URL 리스트
    """
    logger.info(f"Generating latest GDELT URLs for {data_types}")

    # GDELT는 UTC 시간을 기준으로 함
    current_utc_time = datetime.now(timezone.utc)

    # 현재 시각에서 15분 전의 완료된 배치를 가져옴
    target_time = current_utc_time - timedelta(minutes=15)

    # 15분 단위로 정렬
    minute_rounded = (target_time.minute // 15) * 15
    target_time_rounded = target_time.replace(
        minute=minute_rounded, second=0, microsecond=0
    )

    timestamp_str = target_time_rounded.strftime("%Y%m%d%H%M%S")

    base_url = "http://data.gdeltproject.org/gdeltv2/"
    gdelt_urls = {data_type: [] for data_type in data_types}

    # 각 데이터 타입별 URL 생성
    for data_type in data_types:
        if data_type not in FILE_EXTENSIONS:
            logger.warning(f"Unknown data type: {data_type}")
            continue

        file_name = f"{timestamp_str}{FILE_EXTENSIONS[data_type]}"
        download_url = f"{base_url}{file_name}"
        gdelt_urls[data_type].append(download_url)

        logger.info(f"Generated URL for {data_type.upper()}: {download_url}")

    return gdelt_urls


def send_bronze_data_to_kafka(url: str, data_type: str, producer: Producer) -> int:
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
        logger.info(f"Downloading {data_type.upper()} data from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        topic = KAFKA_TOPICS[data_type]

        # 메모리 상에서 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            logger.info(f"Processing {data_type.upper()} file: {csv_filename}")

            with z.open(csv_filename) as c:
                # CSV 파일을 한 줄씩 읽어서 처리
                # GDELT CSV는 탭으로 구분되어 있고, 헤더가 없음
                reader = csv.reader(io.TextIOWrapper(c, "utf-8"), delimiter="\t")

                record_count = 0
                batch_records = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # bronze 데이터에 메타데이터 추가
                        current_utc = datetime.now(timezone.utc)
                        bronze_record = {
                            "data_type": data_type,
                            "bronze_data": row,  # 전체 컬럼을 리스트로
                            "row_number": row_num,
                            "source_file": csv_filename,
                            "extracted_time": current_utc.strftime("%Y-%m-%d %H:%M:%S"),
                            "producer_timestamp": current_utc.isoformat(),  # ★ 중복 제거용 타임스탬프
                            "source_url": url,
                            "total_columns": len(row),
                        }

                        batch_records.append(bronze_record)
                        record_count += 1

                        # 배치 전송 (100개씩)
                        if len(batch_records) >= 100:
                            for record in batch_records:
                                key = str(record["bronze_data"][0])
                                producer.produce(
                                    topic,
                                    value=json.dumps(record, default=str),
                                    key=key
                                )
                            producer.flush()  # 배치 확인 응답 대기
                            batch_records = []

                        # 진행상황 로그 (1000개마다)
                        if record_count % 1000 == 0:
                            logger.info(
                                f"Sent {record_count} {data_type.upper()} records..."
                            )

                    except Exception as e:
                        logger.warning(
                            f"Error processing {data_type} row {row_num}: {e}"
                        )
                        continue

                # 남은 배치 전송
                if batch_records:
                    for record in batch_records:
                        key = str(record["bronze_data"][0])
                        producer.produce(
                            topic,
                            value=json.dumps(record, default=str),
                            key=key
                        )
                    producer.flush()  # 최종 배치 확인 응답 대기
        logger.info(
            f"Successfully sent {record_count} {data_type.upper()} records to {topic}"
        )

        return record_count

    except Exception as e:
        logger.error(
            f"Error processing {data_type} data from {url}: {e}", exc_info=True
        )
        return 0


def process_data_type(data_type: str, urls: List[str], producer: Producer) -> int:
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
    logger.info(f"Starting {data_type.upper()} data processing ({len(urls)} files)")

    for i, url in enumerate(urls, 1):
        logger.info(f"Processing {data_type} file {i}/{len(urls)}")

        try:
            record_count = send_bronze_data_to_kafka(url, data_type, producer)
            total_processed += record_count
            logger.info(f"{data_type} file {i} completed: {record_count:,} records")

        except Exception as e:
            logger.warning(f"Failed to process {data_type} file {url}: {e}")
            continue

        # 진행상황 로그 (5개 파일마다)
        if i % 5 == 0:
            logger.info(
                f"{data_type.upper()} Progress: {i}/{len(urls)} files, {total_processed:,} records"
            )

    logger.info(
        f"{data_type.upper()} completed: {total_processed:,} records from {len(urls)} files"
    )
    return total_processed


def main():
    logger.info("Starting GDELT 3-Way Bronze Data Producer...")

    producer = None
    total_stats = {}

    try:
        # Kafka Producer 생성
        producer = get_kafka_producer()
        logger.info("Kafka producer created successfully")

        # 수집할 데이터 타입 설정
        data_types = ["events", "mentions", "gkg"]

        # 최신 URL 목록 생성 (15분 전 완료된 배치)
        gdelt_urls = get_latest_gdelt_urls(data_types)

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in gdelt_urls or not gdelt_urls[data_type]:
                logger.warning(f"No URLs found for {data_type}")
                continue

            processed_count = process_data_type(
                data_type, gdelt_urls[data_type], producer
            )
            total_stats[data_type] = processed_count

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info("GDELT 3-Way Data Collection Summary:")
        logger.info(f"{'='*50}")

        grand_total = 0
        for data_type, count in total_stats.items():
            logger.info(
                f"{data_type.upper()}: {count:,} records → {KAFKA_TOPICS[data_type]}"
            )
            grand_total += count

        logger.info(f"GRAND TOTAL: {grand_total:,} records collected!")

    except Exception as e:
        logger.error(f"Failed to initialize producer: {e}", exc_info=True)

    finally:
        if producer:
            producer.flush()  # 남은 메시지 전송 대기
            logger.info("Producer flushed and finished successfully")


if __name__ == "__main__":
    main()
