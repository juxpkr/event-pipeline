"""
시간별 GDELT 데이터 수집기 (마이크로 배치용)
- daily_gdelt_collector를 시간 단위로 개선
- 1시간 = 4개 15분 배치 (더 가벼움)
"""

import os
import sys
import requests
import zipfile
import io
import csv
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 프로젝트 루트 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from src.processing.partitioning.gdelt_partition_writer import write_to_delta_lake

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# GDELT 파일 확장자
FILE_EXTENSIONS = {
    "events": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}

# Bronze 경로
BRONZE_PATHS = {
    "events": "s3a://warehouse/bronze/gdelt_events",
    "mentions": "s3a://warehouse/bronze/gdelt_mentions",
    "gkg": "s3a://warehouse/bronze/gdelt_gkg",
}

def get_hourly_gdelt_urls(hour_str: str):
    """시간별 GDELT URL 생성 (15분 간격 4개)"""
    # hour_str 형식: "2023-08-01-14"
    date_hour = datetime.strptime(hour_str, "%Y-%m-%d-%H")
    base_url = "http://data.gdeltproject.org/gdeltv2/"

    urls = {"events": [], "mentions": [], "gkg": []}

    # 1시간 = 4개 15분 배치 (00, 15, 30, 45분)
    for minute in [0, 15, 30, 45]:
        current_time = date_hour.replace(minute=minute, second=0, microsecond=0)
        timestamp = current_time.strftime("%Y%m%d%H%M%S")

        for data_type in urls.keys():
            filename = f"{timestamp}{FILE_EXTENSIONS[data_type]}"
            urls[data_type].append(f"{base_url}{filename}")

    logger.info(f"Generated {len(urls['events'])} URLs per data type for {hour_str}")
    return urls

def download_and_parse_gdelt(url: str, data_type: str, logical_hour: str) -> list:
    """GDELT 파일 다운로드하고 파싱 (기존 Producer 로직 완전 이식)"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {data_type.upper()} data from: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            break
        except (requests.RequestException, Exception) as e:
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"All {max_retries} download attempts failed for {url}")
                return []
            time.sleep(2 ** attempt)  # 지수 백오프

    records = []

    try:
        # 메모리 상에서 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            logger.info(f"Processing {data_type.upper()} file: {csv_filename}")

            with z.open(csv_filename) as c:
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

                for row_num, row in enumerate(reader, 1):
                    try:
                        # bronze 데이터에 메타데이터 추가 (기존 Producer와 동일)
                        current_utc = datetime.now(timezone.utc)
                        # 기존 Bronze 스키마와 호환 (processed_at 제외)
                        bronze_record = {
                            "data_type": data_type,
                            "bronze_data": row,  # 전체 컬럼을 리스트로
                            "row_number": row_num,
                            "source_file": csv_filename,
                            "extracted_time": logical_hour,
                            "producer_timestamp": current_utc.isoformat(),  # 중복 제거용 타임스탬프
                            "source_url": url,
                            "total_columns": len(row),
                        }
                        records.append(bronze_record)

                    except Exception as e:
                        logger.warning(f"Error processing {data_type} row {row_num}: {e}")
                        continue

        logger.info(f"Successfully processed {len(records)} {data_type.upper()} records from {url}")
        return records

    except Exception as e:
        logger.error(f"Error processing {data_type} data from {url}: {e}", exc_info=True)
        return []

def save_to_bronze(spark: SparkSession, records: list, data_type: str):
    """Bronze Layer에 저장 (기존 partition writer 사용)"""
    if not records:
        logger.warning(f"No records to save for {data_type}")
        return

    # 기존 Bronze 스키마 사용 (processed_at 없음)
    schema = StructType([
        StructField("data_type", StringType(), True),
        StructField("bronze_data", ArrayType(StringType()), True),
        StructField("row_number", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("extracted_time", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("total_columns", IntegerType(), True),
    ])

    df = spark.createDataFrame(records, schema)

    # Primary key 추출 (첫 번째 컬럼)
    if data_type == "gkg":
        merge_key = "GKGRECORDID"
        df = df.withColumn("GKGRECORDID", F.col("bronze_data")[0])
    else:
        merge_key = "GLOBALEVENTID"
        df = df.withColumn("GLOBALEVENTID", F.col("bronze_data")[0])

    # 기존 partition writer 사용 (processed_at 자동 추가됨)
    write_to_delta_lake(
        df=df,
        delta_path=BRONZE_PATHS[data_type],
        table_name=f"Bronze {data_type}",
        partition_col="processed_at",  # write_to_delta_lake가 자동 생성
        merge_key=merge_key,
    )

    logger.info(f"Saved {len(records)} {data_type} records to {BRONZE_PATHS[data_type]}")

def collect_hourly_data(hour_str: str):
    """시간별 GDELT 데이터 수집 (기존 Producer 로직 완전 이식)"""
    logger.info(f"Starting hourly collection for {hour_str}")

    spark = get_spark_session(f"Hourly_GDELT_Collector_{hour_str}", "spark://spark-master:7077")
    redis_client.register_driver_ui(spark, f"Hourly Collector {hour_str}")

    total_stats = {}
    all_types_successful = True

    try:
        # 1. URL 생성
        hourly_urls = get_hourly_gdelt_urls(hour_str)
        data_types = ["events", "mentions", "gkg"]

        if not any(hourly_urls.values()):
            logger.info("No data to process for the given hour. Finishing job.")
            return

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in hourly_urls or not hourly_urls[data_type]:
                logger.warning(f"No URLs found for {data_type}")
                continue

            try:
                urls = hourly_urls[data_type]
                logger.info(f"Starting {data_type.upper()} data processing ({len(urls)} files)")

                all_records = []
                success_count = 0

                for i, url in enumerate(urls, 1):
                    logger.info(f"Processing {data_type} file {i}/{len(urls)}")

                    try:
                        records = download_and_parse_gdelt(url, data_type, hour_str)
                        if records:
                            all_records.extend(records)
                            success_count += 1
                            logger.info(f"{data_type} file {i} completed: {len(records):,} records")
                        else:
                            logger.warning(f"No records from {data_type} file {i}")

                    except Exception as e:
                        logger.warning(f"Failed to process {data_type} file {url}: {e}")
                        continue

                # Bronze에 저장
                save_to_bronze(spark, all_records, data_type)

                total_stats[data_type] = {
                    "record_count": len(all_records),
                    "url_count": len(urls),
                    "success_count": success_count
                }

                logger.info(f"{data_type.upper()} completed: {len(all_records):,} records from {success_count}/{len(urls)} files")

            except Exception as e:
                logger.error(f"Failed to process data type: {data_type}. Reason: {e}")
                all_types_successful = False
                total_stats[data_type] = {
                    "record_count": 0,
                    "url_count": len(hourly_urls.get(data_type, [])),
                    "success_count": 0
                }

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info(f"Hourly Collection Summary for {hour_str}:")
        logger.info(f"{'='*50}")

        grand_total = 0
        for data_type, stats in total_stats.items():
            record_count = stats.get('record_count', 0)
            success_count = stats.get('success_count', 0)
            url_count = stats.get('url_count', 0)
            logger.info(f"{data_type.upper()}: {record_count:,} records ({success_count}/{url_count} files)")
            grand_total += record_count

        logger.info(f"GRAND TOTAL: {grand_total:,} records collected!")

        # 부분 실패 허용: 최소 하나의 데이터 타입이라도 성공하면 OK
        success_count = sum(1 for stats in total_stats.values() if stats.get('record_count', 0) > 0)
        if success_count == 0:
            raise Exception("Complete failure: No data was collected for any data type.")
        elif not all_types_successful:
            logger.warning(f"Partial success: {success_count}/{len(total_stats)} data types succeeded.")

        logger.info(f"Hourly collection completed successfully for {hour_str}")

    except Exception as e:
        logger.error(f"Error in hourly collection: {e}", exc_info=True)
        raise

    finally:
        redis_client.unregister_driver_ui(spark)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: hourly_gdelt_collector.py <YYYY-MM-DD-HH>")
        sys.exit(1)

    hour_str = sys.argv[1]

    # 시간 형식 검증
    try:
        datetime.strptime(hour_str, "%Y-%m-%d-%H")
    except ValueError:
        logger.error(f"Invalid hour format: {hour_str}. Use YYYY-MM-DD-HH")
        sys.exit(1)

    collect_hourly_data(hour_str)