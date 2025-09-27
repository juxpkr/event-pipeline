"""
GDELT 백필용 Producer
기존 Producer 로직을 재사용하되, 체크포인트 무시하고 직접 날짜 범위 지정
"""

import os
import requests
import zipfile
import io
import csv
import json
import time
import logging
import argparse
import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Producer
from dotenv import load_dotenv
from src.utils.kafka_producer import get_kafka_producer
from src.validation.lifecycle_metrics_exporter import export_producer_collection_metrics
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


def get_gdelt_urls_for_period(
    start_time_str: str,
    end_time_str: str,
    data_types: List[str] = ["events", "mentions", "gkg"],
) -> Dict[str, List[str]]:
    """
    GDELT 3가지 데이터 타입의 URL을 생성 (백필용)
    주어진 기간(start ~ end) 동안의 모든 15분 배치 GDELT URL을 생성

    Args:
        start_time_str: 시작 시간 (YYYY-MM-DD HH:MM:SS)
        end_time_str: 종료 시간 (YYYY-MM-DD HH:MM:SS)
        data_types: 수집할 데이터 타입 ['events', 'mentions', 'gkg']

    Returns:
        Dict[str, List[str]]: 데이터 타입별 URL 리스트
    """
    logger.info(f"Generating GDELT URLs from {start_time_str} to {end_time_str}")
    logger.info(f"Generating GDELT URLs for {data_types}")

    # 시간대 정보 확인 및 UTC 변환
    start_time = datetime.fromisoformat(start_time_str)
    end_time = datetime.fromisoformat(end_time_str)

    if start_time >= end_time:
        logger.warning(
            f"Start time ({start_time_str}) is not before end time ({end_time_str}). No data to process."
        )
        return {"events": [], "mentions": [], "gkg": []}

    # UTC로 변환 (timezone naive면 UTC로 가정)
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    # GDELT 15분 단위로 정렬 (00, 15, 30, 45분)
    minute_rounded = (start_time.minute // 15) * 15
    current_time = start_time.replace(minute=minute_rounded, second=0, microsecond=0)

    gdelt_urls = {"events": [], "mentions": [], "gkg": []}
    base_url = "http://data.gdeltproject.org/gdeltv2/"

    while current_time < end_time:
        timestamp_str = current_time.strftime("%Y%m%d%H%M%S")
        for data_type in gdelt_urls.keys():
            file_name = f"{timestamp_str}{FILE_EXTENSIONS[data_type]}"
            download_url = f"{base_url}{file_name}"
            gdelt_urls[data_type].append(download_url)

        current_time += timedelta(minutes=15)  # 15분씩 증가

    for data_type, urls in gdelt_urls.items():
        logger.info(f"Generated {len(urls)} URLs for {data_type.upper()}")

    return gdelt_urls


def send_bronze_data_to_kafka(
    url: str, data_type: str, producer: Producer, logical_date: str
) -> int:
    """
    URL에서 GDELT 데이터를 다운로드하고, 데이터 타입별 토픽으로 전송

    Args:
        url: GDELT 데이터 다운로드 URL
        data_type: 데이터 타입 ('events', 'mentions', 'gkg')
        producer: Kafka Producer 인스턴스
        logical_date: 백필 대상 날짜

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

                # UTF-8 -> latin-1 -> UTF-8 에러무시 순서로 시도
                reader = None
                try:
                    reader = csv.reader(io.TextIOWrapper(c, "utf-8"), delimiter="\t")
                except UnicodeDecodeError:
                    try:
                        c.seek(0)
                        reader = csv.reader(io.TextIOWrapper(c, "latin-1"), delimiter="\t")
                        logger.info(f"Using latin-1 encoding for {csv_filename}")
                    except UnicodeDecodeError:
                        c.seek(0)
                        reader = csv.reader(io.TextIOWrapper(c, "utf-8", errors='ignore'), delimiter="\t")
                        logger.warning(f"Using UTF-8 with error ignore for {csv_filename}")

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
                            "extracted_time": logical_date,
                            "producer_timestamp": current_utc.isoformat(),  # 중복 제거용 타임스탬프
                            "processed_at": logical_date,  # 백필 대상 날짜
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
                                    key=key,
                                )
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
                            topic, value=json.dumps(record, default=str), key=key
                        )
        logger.info(
            f"Successfully sent {record_count} {data_type.upper()} records to {topic}"
        )

        return record_count

    except Exception as e:
        logger.error(
            f"Error processing {data_type} data from {url}: {e}", exc_info=True
        )
        return 0


def process_data_type(
    data_type: str, urls: List[str], producer: Producer, logical_date: str
) -> int:
    """
    events, mentions, gkg 데이터의 모든 URL을 처리

    Args:
        data_type: 데이터 타입
        urls: 처리할 URL 리스트
        producer: Kafka Producer
        logical_date: 백필 대상 날짜

    Returns:
        int: 총 처리된 레코드 수
    """
    total_processed = 0
    logger.info(f"Starting {data_type.upper()} data processing ({len(urls)} files)")

    for i, url in enumerate(urls, 1):
        logger.info(f"Processing {data_type} file {i}/{len(urls)}")

        try:
            record_count = send_bronze_data_to_kafka(
                url, data_type, producer, logical_date
            )
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
    # 커맨드라인 인자 파싱
    parser = argparse.ArgumentParser(description="GDELT 백필용 Producer")
    parser.add_argument(
        "--start-date",
        required=True,
        help="백필 시작 날짜 (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="백필 종료 날짜 (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--logical-date",
        help="논리적 처리 날짜 (기본값: start-date)",
    )
    args = parser.parse_args()

    start_time_str = args.start_date
    end_time_str = args.end_date
    logical_date = args.logical_date or args.start_date

    logger.info(f"Starting GDELT Backfill Producer")
    logger.info(f"Period: {start_time_str} to {end_time_str}")
    logger.info(f"Logical date: {logical_date}")

    producer = None
    all_types_successful = True
    total_stats = {}

    try:
        # STEP 1: 시작/종료 시간 기반으로 처리해야 할 모든 URL 목록을 생성한다.
        data_types = ["events", "mentions", "gkg"]
        gdelt_urls = get_gdelt_urls_for_period(start_time_str, end_time_str, data_types)

        if not any(gdelt_urls.values()):
            logger.info("No data to process for the given period. Finishing job.")
            return

        # Kafka Producer 생성
        producer = get_kafka_producer()
        logger.info("Kafka producer created successfully")

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in gdelt_urls or not gdelt_urls[data_type]:
                logger.warning(f"No URLs found for {data_type}")
                continue

            try:
                processed_count = process_data_type(
                    data_type, gdelt_urls[data_type], producer, logical_date
                )
                total_stats[data_type] = {
                    "record_count": processed_count,
                    "url_count": len(gdelt_urls[data_type])
                }
            except Exception as e:
                logger.error(
                    f"Failed to process data type: {data_type}. Reason: {e}"
                )
                all_types_successful = False
                total_stats[data_type] = {
                    "record_count": 0,
                    "url_count": len(gdelt_urls.get(data_type, []))
                }

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info("GDELT Backfill Collection Summary:")
        logger.info(f"{'='*50}")

        grand_total = 0
        for data_type, stats in total_stats.items():
            record_count = stats.get('record_count', 0) if isinstance(stats, dict) else stats
            logger.info(
                f"{data_type.upper()}: {record_count:,} records → {KAFKA_TOPICS[data_type]}"
            )
            grand_total += record_count

        logger.info(f"GRAND TOTAL: {grand_total:,} records collected!")

        if not all_types_successful:
            raise Exception("Backfill job failed due to partial success.")

    except Exception as e:
        logger.error(f"An error occurred during backfill execution: {e}", exc_info=True)
        raise e

    finally:
        if producer:
            producer.flush()  # 남은 메시지 전송 대기
            logger.info("Producer flushed.")


if __name__ == "__main__":
    main()