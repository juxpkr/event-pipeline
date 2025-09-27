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

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
CHECKPOINT_BUCKET = "warehouse"
CHECKPOINT_KEY = "checkpoints/producer/last_success_timestamp.txt"

# 3가지 데이터 타입별 토픽 (백필용)
KAFKA_TOPICS = {
    "events": "gdelt_events_backfill",
    "mentions": "gdelt_mentions_backfill",
    "gkg": "gdelt_gkg_backfill",
}

# GDELT 데이터 타입별 파일 확장자
FILE_EXTENSIONS = {
    "events": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}

# MinIO 클라이언트 생성
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD,
    config=boto3.session.Config(signature_version="s3v4"),
)


def get_last_success_timestamp(default_start_time: str) -> str:
    """MinIO에서 마지막 성공 타임스탬프를 읽어온다."""
    try:
        response = s3_client.get_object(Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY)
        last_success_time = response["Body"].read().decode("utf-8").strip()
        logger.info(f"Checkpoint found. Last successful timestamp: {last_success_time}")
        return last_success_time
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.warning("Checkpoint file not found. Using default start time.")
            return default_start_time
        else:
            raise


def save_success_timestamp(timestamp: str):
    """MinIO에 성공 타임스탬프를 저장한다."""
    s3_client.put_object(
        Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY, Body=timestamp.encode("utf-8")
    )
    logger.info(f"Checkpoint updated successfully with timestamp: {timestamp}")


def get_gdelt_urls_for_period(
    start_time_str: str,
    end_time_str: str,
    data_types: List[str] = ["events", "mentions", "gkg"],
) -> Dict[str, List[str]]:
    """
    GDELT 3가지 데이터 타입의 URL을 생성 (단일 15분 배치)
    주어진 기간(start ~ end) 동안의 모든 15분 배치 GDELT URL을 생성
    기간은 minio checkpoint에서 읽어옴

    Args:
        data_types: 수집할 데이터 타입 ['events', 'mentions', 'gkg']

    Returns:
        Dict[str, List[str]]: 데이터 타입별 URL 리스트
    """
    logger.info(f"Generating GDELT URLs from {start_time_str} to {end_time_str}")
    logger.info(f"Generating latest GDELT URLs for {data_types}")

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
                        reader = csv.reader(
                            io.TextIOWrapper(c, "latin-1"), delimiter="\t"
                        )
                        logger.info(f"Using latin-1 encoding for {csv_filename}")
                    except UnicodeDecodeError:
                        c.seek(0)
                        reader = csv.reader(
                            io.TextIOWrapper(c, "utf-8", errors="ignore"),
                            delimiter="\t",
                        )
                        logger.warning(
                            f"Using UTF-8 with error ignore for {csv_filename}"
                        )

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
                            "processed_at": logical_date,  # Airflow logical date
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
    parser = argparse.ArgumentParser(description="GDELT 3-Way Bronze Data Producer")
    # 기존 logical-date는 Airflow 연동을 위해 유지
    parser.add_argument(
        "--logical-date",
        required=True,
        help="Airflow logical date (data_interval_start)",
    )
    # 백필 모드를 위한 옵션 추가
    parser.add_argument("--backfill-start", help="Backfill mode start time (YYYY-MM-DDTHH:MM:SS)")
    parser.add_argument("--backfill-end", help="Backfill mode end time (YYYY-MM-DDTHH:MM:SS)")
    args = parser.parse_args()

    producer = None
    all_types_successful = True
    total_stats = {}
    try:
        if args.backfill_start and args.backfill_end:
            # === 백필 모드 ===
            logger.info(f"Running in BACKFILL mode for period: {args.backfill_start} to {args.backfill_end}")
            start_time_str = args.backfill_start
            end_time_str = args.backfill_end
            # 🚨 중요: 백필 모드에서는 체크포인트를 사용하지 않고, 업데이트도 하지 않는다!
            all_types_successful = True # 체크포인트 로직을 건너뛰기 위한 플래그
        else:
            # === 실시간 모드 (기존 로직) ===
            logger.info("Running in REAL-TIME mode using checkpoints.")
            end_time_str = args.logical_date

            # 처음 파이프라인이 돌 때 시간 설정: 1시간 전 데이터부터 수집 시작
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

            # GDELT 시간에 맞게 15분 단위로 정렬
            minute_rounded = (one_hour_ago.minute // 15) * 15
            default_start_time_obj = one_hour_ago.replace(
                minute=minute_rounded, second=0, microsecond=0
            )
            default_start_time = default_start_time_obj.isoformat()

            # 이제 get_last_success_timestamp 함수는 첫 실행 시 이 '1시간 전' 값을 사용하게 된다.
            start_time_str = get_last_success_timestamp(default_start_time)
            all_types_successful = True # 초기값

        # STEP 2: 시작 시간부터 끝나는 시간까지 처리해야 할 모든 URL 목록을 생성한다.
        data_types = ["events", "mentions", "gkg"]
        gdelt_urls = get_gdelt_urls_for_period(start_time_str, end_time_str, data_types)

        if not any(gdelt_urls.values()):
            logger.info("No new data to process for the given period. Finishing job.")
            save_success_timestamp(
                end_time_str
            )  # 처리할게 없어도 체크포인트는 업데이트 해야함
            return

        # Kafka Producer 생성
        producer = get_kafka_producer()
        logger.info("Kafka producer created successfully")

        # 수집할 데이터 타입 설정
        data_types = ["events", "mentions", "gkg"]

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in gdelt_urls or not gdelt_urls[data_type]:
                logger.warning(f"No URLs found for {data_type}")
                continue

            try:
                # 개별 작업도 try-except로 감싸서, 하나라도 실패하면 Flag를 False 설정
                processed_count = process_data_type(
                    data_type, gdelt_urls[data_type], producer, args.logical_date
                )
                total_stats[data_type] = {
                    "record_count": processed_count,
                    "url_count": len(gdelt_urls[data_type]),
                }
            except Exception as e:
                logger.error(
                    f"!!! FAILED to process data type: {data_type}. Reason: {e}"
                )
                all_types_successful = False
                total_stats[data_type] = {
                    "record_count": 0,
                    "url_count": len(gdelt_urls.get(data_type, [])),
                }

        # STEP 3: 체크포인트 업데이트는 실시간 모드에서만!
        if not (args.backfill_start and args.backfill_end) and all_types_successful:
            save_success_timestamp(end_time_str)
            logger.info("All data types processed successfully. Checkpoint updated.")
        elif not (args.backfill_start and args.backfill_end):
            # 실시간 모드에서 하나라도 실패했다면 체크포인트를 절대 업데이트하면 안됨!
            logger.error(
                "One or more data types failed. Checkpoint will NOT be updated."
            )
            # 실패를 Airflow에 명확히 알리기 위해 에러 발생
            raise Exception("Producer job failed due to partial success.")
        else:
            # 백필 모드에서는 체크포인트 업데이트 하지 않음
            logger.info("Backfill mode completed. Checkpoint not updated.")

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info("GDELT 3-Way Data Collection Summary:")
        logger.info(f"{'='*50}")

        grand_total = 0
        for data_type, stats in total_stats.items():
            record_count = (
                stats.get("record_count", 0) if isinstance(stats, dict) else stats
            )
            logger.info(
                f"{data_type.upper()}: {record_count:,} records → {KAFKA_TOPICS[data_type]}"
            )
            grand_total += record_count

        logger.info(f"GRAND TOTAL: {grand_total:,} records collected!")

        # Producer 수집 통계를 Prometheus로 전송
        try:
            export_producer_collection_metrics(total_stats)
            logger.info("Collection metrics exported to Prometheus successfully")
        except Exception as e:
            logger.warning(f"Failed to export collection metrics: {e}")

    except Exception as e:
        logger.error(f"An error occurred during producer execution: {e}", exc_info=True)
        # 실패 시 체크포인트 업데이트를 하지 않으므로, 다음 실행 때 이 구간을 재시도하게 된다.
        raise e  # Airflow가 실패를 인지하도록 에러를 다시 발생시킨다.

    finally:
        if producer:
            producer.flush()  # 남은 메시지 전송 대기
            logger.info("Producer flushed.")


if __name__ == "__main__":
    main()
