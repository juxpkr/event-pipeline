"""
GDELT 백필용 Bronze Consumer
기존 Consumer 로직을 재사용하되, 특정 타임스탬프 범위만 처리
.trigger(availableNow=True) 사용하여 모든 사용 가능한 데이터를 한 번에 처리
"""

import os
import sys
import time
import argparse
from pathlib import Path
import logging
from datetime import datetime, timezone

# 프로젝트 루트를 Python path에 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

# utils에서 필요한 모듈 임포트
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from src.processing.partitioning.gdelt_partition_writer import write_to_delta_lake
from src.audit.lifecycle_tracker import EventLifecycleTracker

# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- 상수 정의 ---
KAFKA_TOPICS = {
    "events": os.getenv("KAFKA_TOPIC_GDELT_EVENTS", "gdelt_events_bronze"),
    "mentions": os.getenv("KAFKA_TOPIC_GDELT_MENTIONS", "gdelt_mentions_bronze"),
    "gkg": os.getenv("KAFKA_TOPIC_GDELT_GKG", "gdelt_gkg_bronze"),
}

MINIO_PATHS = {
    "events": "s3a://warehouse/bronze/gdelt_events",
    "mentions": "s3a://warehouse/bronze/gdelt_mentions",
    "gkg": "s3a://warehouse/bronze/gdelt_gkg",
}

# Bronze 데이터 스키마 정의: Kafka 메시지 파싱용
BRONZE_SCHEMA = StructType(
    [
        StructField("data_type", StringType(), True),
        StructField("bronze_data", ArrayType(StringType()), True),
        StructField("row_number", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("extracted_time", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
        StructField("processed_at", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("total_columns", IntegerType(), True),
    ]
)

# 데이터 타입별 설정을 딕셔너리로 분리
DATA_TYPE_CONFIG = {
    "events": {"pk_index": 0, "merge_key": "GLOBALEVENTID"},
    "mentions": {"pk_index": 0, "merge_key": "GLOBALEVENTID"},
    "gkg": {"pk_index": 0, "merge_key": "GKGRECORDID"},
}


def setup_backfill_query(spark: SparkSession, data_type: str, start_time: str, end_time: str, logger):
    """
    지정된 데이터 타입에 대한 백필 스트리밍 쿼리를 설정하고 시작
    특정 타임스탬프 범위의 데이터만 처리
    """
    # 변수 설정
    kafka_topic = KAFKA_TOPICS[data_type]
    minio_path = MINIO_PATHS[data_type]
    checkpoint_path = f"s3a://warehouse/checkpoints/backfill/{data_type}_{int(datetime.now().timestamp())}"
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Lifecycle tracker 초기화
    lifecycle_tracker = EventLifecycleTracker(spark)

    # MERGE를 시도하기 전에, 테이블이 존재하는지 먼저 확인하고, 없으면 생성
    table_path = "s3a://warehouse/audit/lifecycle"
    try:
        spark.catalog.tableExists(f"delta.`{table_path}`")
        spark.read.format("delta").load(table_path).limit(1).collect()
    except:
        logger.info("Lifecycle table not found. Initializing...")
        lifecycle_tracker.initialize_table()

    # 1. readStream으로 Kafka 데이터 읽기
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    def process_micro_batch(df: DataFrame, epoch_id: int):
        """
        Kafka에서 받은 마이크로 배치를 처리하고 Bronze에 저장.
        백필용이므로 특정 시간 범위의 데이터만 필터링.
        """
        logger.info(f"--- Starting Backfill Batch {epoch_id} for {data_type} ---")
        record_count = 0

        try:
            # 빈 배치는 즉시 종료
            if df.isEmpty():
                logger.info(f"Batch {epoch_id} is empty. Skipping.")
                return

            # 후속 작업을 위해 캐싱
            df.cache()
            record_count = df.count()

            logger.info(
                f"Processing {record_count} records for '{data_type}' in backfill batch {epoch_id}."
            )

            # Kafka 메시지 파싱
            config = DATA_TYPE_CONFIG[data_type]
            merge_key_name = config["merge_key"]
            pk_col_index = config["pk_index"]

            parsed_df = df.select(
                from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("data")
            ).select("data.*")

            # 백필 시간 범위 필터링 추가
            df_time_filtered = parsed_df.filter(
                (col("processed_at") >= start_time) & (col("processed_at") <= end_time)
            )

            time_filtered_count = df_time_filtered.count()
            logger.info(f"After time filtering ({start_time} to {end_time}): {time_filtered_count} records")

            if time_filtered_count == 0:
                logger.info("No records in target time range. Skipping batch.")
                return

            df_with_keys = df_time_filtered.withColumn(
                merge_key_name, F.trim(col("bronze_data").getItem(pk_col_index))
            ).withColumn(
                "processed_at",
                to_timestamp(col("processed_at"), "yyyy-MM-dd HH:mm:ss"),
            )

            df_validated = df_with_keys.filter(
                F.col(merge_key_name).isNotNull() & (F.col(merge_key_name) != "")
            )

            validated_count = df_validated.count()
            dropped_count = time_filtered_count - validated_count
            if dropped_count > 0:
                logger.warning(f"Dropped {dropped_count} records due to NULL/EMPTY key.")

            if validated_count > 0:
                # Bronze 저장
                write_to_delta_lake(
                    df=df_validated,
                    delta_path=minio_path,
                    table_name=f"Bronze {data_type} (Backfill)",
                    partition_col="processed_at",
                    merge_key=merge_key_name,
                )

                # events와 gkg만 lifecycle 추적
                if data_type in ["events", "gkg"]:
                    try:
                        df_for_lifecycle = df_validated.withColumnRenamed(
                            merge_key_name, "global_event_id"
                        )
                        event_type_for_tracker = (
                            "EVENT" if data_type == "events" else "GKG"
                        )

                        tracked_count = lifecycle_tracker.track_bronze_arrival(
                            events_df=df_for_lifecycle,
                            batch_id=f"{data_type}_backfill_batch_{epoch_id}",
                            event_type=event_type_for_tracker,
                        )
                        logger.info(
                            f"Successfully tracked {tracked_count} events in lifecycle for backfill batch {epoch_id}."
                        )
                    except Exception as e:
                        logger.error(
                            f"LIFECYCLE TRACKING FAILED for backfill batch {epoch_id}: {e}"
                        )
                        raise
                else:
                    logger.info(
                        f"Skipping lifecycle tracking for '{data_type}' to prevent race conditions."
                    )

        except Exception as e:
            logger.error(
                f"CRITICAL ERROR in backfill batch {epoch_id} for {data_type}: {str(e)}"
            )
            raise e

        finally:
            # 캐시 해제
            df.unpersist()
            logger.info(
                f"--- Finished Backfill Batch {epoch_id} for {data_type}, Processed {record_count} records ---"
            )

    # writeStream 실행 - availableNow=True로 모든 사용 가능한 데이터를 한 번에 처리
    query = (
        kafka_df.writeStream.foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    logger.info(f"[{data_type.upper()}] [SUCCESS] Backfill streaming batch job started.")

    return query


def main():
    """
    GDELT 백필용 Bronze Consumer 메인 함수
    """
    # 커맨드라인 인자 파싱
    parser = argparse.ArgumentParser(description="GDELT 백필용 Bronze Consumer")
    parser.add_argument(
        "--start-time",
        required=True,
        help="백필 시작 시간 (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--end-time",
        required=True,
        help="백필 종료 시간 (YYYY-MM-DD HH:MM:SS)",
    )
    args = parser.parse_args()

    spark = get_spark_session("GDELT_Bronze_Consumer_Backfill")

    # Redis에 Spark Driver UI 정보 등록
    redis_client.register_driver_ui(spark, f"GDELT Bronze Consumer Backfill ({args.start_time} to {args.end_time})")

    # 스파크의 log4j 로거를 사용
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("GDELT_BRONZE_CONSUMER_BACKFILL")

    logger.info("========== Starting GDELT Backfill Bronze Consumer ==========")
    logger.info(f"Backfill period: {args.start_time} to {args.end_time}")

    queries = []
    try:
        # STEP 1: 세 개의 스트림을 모두 시작
        for data_type in ["events", "mentions", "gkg"]:
            logger.info(f"Setting up backfill stream for data type: {data_type.upper()}")

            query = setup_backfill_query(spark, data_type, args.start_time, args.end_time, logger)
            queries.append(query)

        # STEP 2: 모든 스트림이 끝날 때까지 대기
        while any([q.isActive for q in queries]):
            time.sleep(0.5)

        # 실패한 쿼리 체크
        for query in queries:
            if query.exception():
                failed_query_name = query.name if query.name else "Unknown Query"
                error_message = str(query.exception())
                logger.error(
                    f"CRITICAL FAILURE DETECTED in backfill query '{failed_query_name}'"
                )
                logger.error(f"Error: {error_message}")

                # 실패가 감지되면, 아직 실행 중인 다른 모든 쿼리를 즉시 중단
                logger.warning("Stopping all other active queries to ensure atomicity...")
                for q in queries:
                    if q.isActive:
                        q.stop()

                raise query.exception()

        logger.info(
            "========== GDELT Backfill Bronze Consumer FINISHED SUCCESSFULLY =========="
        )

    except Exception as e:
        logger.error(f"GDELT Backfill Bronze Consumer FAILED: {e}")
        raise e
    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        logger.info("Spark session closed")
        spark.stop()


if __name__ == "__main__":
    main()