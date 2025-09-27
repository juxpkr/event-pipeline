"""
일일 GDELT 데이터 수집기 (마이크로 배치용)
- Kafka 우회하고 직접 GDELT 서버 → Bronze Layer
- 하루치 데이터만 처리하므로 예측 가능하고 안정적
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

def get_daily_gdelt_urls(date_str: str):
    """하루치 GDELT URL 생성 (15분 간격)"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    base_url = "http://data.gdeltproject.org/gdeltv2/"

    urls = {"events": [], "mentions": [], "gkg": []}

    # 하루 = 96개 15분 배치 (24시간 * 4)
    current_time = date_obj
    end_time = date_obj + timedelta(days=1)

    while current_time < end_time:
        timestamp = current_time.strftime("%Y%m%d%H%M%S")

        for data_type in urls.keys():
            filename = f"{timestamp}{FILE_EXTENSIONS[data_type]}"
            urls[data_type].append(f"{base_url}{filename}")

        current_time += timedelta(minutes=15)

    logger.info(f"Generated {len(urls['events'])} URLs per data type for {date_str}")
    return urls

def download_and_parse_gdelt(url: str, data_type: str, logical_date: str) -> list:
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
    """Bronze Layer에 저장"""
    if not records:
        logger.warning(f"No records to save for {data_type}")
        return

    # DataFrame 생성
    schema = StructType([
        StructField("data_type", StringType(), True),
        StructField("bronze_data", ArrayType(StringType()), True),
        StructField("row_number", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("extracted_time", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("total_columns", IntegerType(), True),
        StructField("processed_at", StringType(), True),
    ])

    df = spark.createDataFrame(records, schema)

    # 파티션 컬럼 추가
    df = df.withColumn("year", F.year(F.col("processed_at").cast("timestamp"))) \
          .withColumn("month", F.month(F.col("processed_at").cast("timestamp"))) \
          .withColumn("day", F.dayofmonth(F.col("processed_at").cast("timestamp"))) \
          .withColumn("hour", F.hour(F.col("processed_at").cast("timestamp")))

    # Primary key 추출 (첫 번째 컬럼)
    if data_type == "gkg":
        # GKG는 GKGRECORDID가 PK
        df = df.withColumn("GKGRECORDID", F.col("bronze_data")[0])
    else:
        # Events/Mentions는 GLOBALEVENTID가 PK
        df = df.withColumn("GLOBALEVENTID", F.col("bronze_data")[0])

    # Delta Lake에 저장
    bronze_path = BRONZE_PATHS[data_type]

    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("year", "month", "day", "hour") \
        .save(bronze_path)

    logger.info(f"Saved {len(records)} {data_type} records to {bronze_path}")

def collect_daily_data(date_str: str):
    """하루치 GDELT 데이터 수집 (기존 Producer 로직 완전 이식)"""
    logger.info(f"Starting daily collection for {date_str}")

    spark = get_spark_session(f"Daily_GDELT_Collector_{date_str}", "spark://spark-master:7077")
    redis_client.register_driver_ui(spark, f"Daily Collector {date_str}")

    total_stats = {}
    all_types_successful = True

    try:
        # 1. URL 생성
        daily_urls = get_daily_gdelt_urls(date_str)
        data_types = ["events", "mentions", "gkg"]

        if not any(daily_urls.values()):
            logger.info("No data to process for the given date. Finishing job.")
            return

        # 각 데이터 타입별로 순차 처리
        for data_type in data_types:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {data_type.upper()} Data")
            logger.info(f"{'='*50}")

            if data_type not in daily_urls or not daily_urls[data_type]:
                logger.warning(f"No URLs found for {data_type}")
                continue

            try:
                urls = daily_urls[data_type]
                logger.info(f"Starting {data_type.upper()} data processing ({len(urls)} files)")

                all_records = []
                success_count = 0

                for i, url in enumerate(urls, 1):
                    logger.info(f"Processing {data_type} file {i}/{len(urls)}")

                    try:
                        records = download_and_parse_gdelt(url, data_type, date_str)
                        if records:
                            all_records.extend(records)
                            success_count += 1
                            logger.info(f"{data_type} file {i} completed: {len(records):,} records")
                        else:
                            logger.warning(f"No records from {data_type} file {i}")

                    except Exception as e:
                        logger.warning(f"Failed to process {data_type} file {url}: {e}")
                        continue

                    # 진행상황 로그 (5개 파일마다)
                    if i % 5 == 0:
                        logger.info(f"{data_type.upper()} Progress: {i}/{len(urls)} files, {len(all_records):,} records")

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
                    "url_count": len(daily_urls.get(data_type, [])),
                    "success_count": 0
                }

        # 최종 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info(f"Daily Collection Summary for {date_str}:")
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

        logger.info(f"Daily collection completed successfully for {date_str}")

    except Exception as e:
        logger.error(f"Error in daily collection: {e}", exc_info=True)
        raise

    finally:
        redis_client.unregister_driver_ui(spark)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: daily_gdelt_collector.py <YYYY-MM-DD>")
        sys.exit(1)

    date_str = sys.argv[1]

    # 날짜 형식 검증
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD")
        sys.exit(1)

    collect_daily_data(date_str)