"""
GDELT BRONZE CONSUMER - EVENTS ONLY
- Kafka에서 'events' 토픽 하나만 독립적으로 처리
- 데이터 파싱, 정제, 키 추출 (공백, null 처리)
- 파티셔닝 및 MERGE를 통해 멱등성 있게 MinIO Bronze Layer에 저장
- Lifecycle Tracker를 통해 'EVENT' 타입으로 도착 기록
"""

import os
import sys
import time
from pathlib import Path
import logging

# 프로젝트 루트를 Python path에 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

# utils에서 필요한 모듈 임포트
from src.utils.spark_builder import get_spark_session
from src.processing.partitioning.gdelt_partition_writer import write_to_delta_lake
from src.audit.lifecycle_tracker import EventLifecycleTracker


# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- [수정] 상수 정의: events 전용으로 단순화 ---
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GDELT_EVENTS", "gdelt_events_bronze")
MINIO_PATH = "s3a://warehouse/bronze/gdelt_events"
CHECKPOINT_PATH = "s3a://warehouse/checkpoints/bronze/events"
DATA_TYPE = "events"
MERGE_KEY = "GLOBALEVENTID"
PK_COL_INDEX = 0

# Bronze 데이터 스키마 정의 (기존과 동일)
BRONZE_SCHEMA = StructType(
    [
        StructField("data_type", StringType(), True),
        StructField("bronze_data", ArrayType(StringType()), True),
        StructField("row_number", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("extracted_time", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("total_columns", IntegerType(), True),
    ]
)


def process_events_micro_batch(df: DataFrame, epoch_id: int, spark: SparkSession):
    """events 데이터 처리를 위한 배치 함수"""
    logger.info(f"[{DATA_TYPE.upper()}] Processing micro-batch {epoch_id}...")
    if df.isEmpty():
        logger.info(f"[{DATA_TYPE.upper()}] Micro-batch is empty.")
        return

    record_count = df.count()
    logger.info(f"[{DATA_TYPE.upper()}] Read {record_count:,} new records from Kafka.")

    # 데이터 파싱 및 키 추출
    parsed_df = df.select(
        from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("data")
    ).select("data.*")

    df_with_keys = parsed_df.withColumn(
        MERGE_KEY, F.trim(col("bronze_data").getItem(PK_COL_INDEX))
    ).withColumn(
        "processed_at",
        to_timestamp(col("extracted_time"), "yyyy-MM-dd HH:mm:ss"),
    )

    df_validated = df_with_keys.filter(
        F.col(MERGE_KEY).isNotNull() & (F.col(MERGE_KEY) != "")
    )

    validated_count = df_validated.count()
    dropped_count = record_count - validated_count
    if dropped_count > 0:
        logger.warn(
            f"[{DATA_TYPE.upper()}] Dropped {dropped_count} records due to NULL/EMPTY key."
        )

    if validated_count > 0:
        # Bronze 테이블에 저장
        write_to_delta_lake(
            df=df_validated,
            delta_path=MINIO_PATH,
            table_name=f"Bronze {DATA_TYPE}",
            partition_col="processed_at",
            merge_key=MERGE_KEY,
        )

        # Lifecycle tracking: 'EVENT' 타입으로만 기록
        lifecycle_tracker = EventLifecycleTracker(spark)
        df_for_lifecycle = df_validated.withColumnRenamed(MERGE_KEY, "global_event_id")

        tracked_count = lifecycle_tracker.track_bronze_arrival(
            df_for_lifecycle, f"{DATA_TYPE}_batch", "EVENT"
        )
        logger.info(
            f"[{DATA_TYPE.upper()}] Tracked {tracked_count} events as WAITING with type EVENT"
        )


def main():
    """Events Bronze Consumer 메인 함수"""
    spark = get_spark_session(f"GDELT_Bronze_Consumer_{DATA_TYPE.upper()}")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    logger.info(
        f"========== Starting GDELT Bronze Consumer for {DATA_TYPE.upper()} =========="
    )

    try:
        # Kafka 데이터 읽기
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 스트림 실행
        query = (
            kafka_df.writeStream.foreachBatch(
                lambda df, epoch_id: process_events_micro_batch(df, epoch_id, spark)
            )
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(once=True)
            .start()
        )

        query.awaitTermination()

        logger.info(
            f"========== GDELT Bronze Consumer for {DATA_TYPE.upper()} FINISHED SUCCESSFULLY =========="
        )

    except Exception as e:
        logger.error(
            f"!!! GDELT Bronze Consumer for {DATA_TYPE.upper()} FAILED: {e} !!!"
        )
        raise e
    finally:
        logger.info("Spark session closed")
        spark.stop()


if __name__ == "__main__":
    main()
