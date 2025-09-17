"""
GDELT 3-Way Bronze Data Consumer
- Kafka에서 배치로 데이터를 안정적으로 읽기 (group.id 사용)
- 데이터 파싱, 정제, 키 추출 (공백, null 처리)
- 파티셔닝 및 MERGE를 통해 멱등성 있게 MinIO Bronze Layer에 저장
- Delta Lake 형식 사용
- partition_writer 호출
"""

import os
import sys
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
        StructField("producer_timestamp", StringType(), True),  # ★ 새 컬럼 추가
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


def process_kafka_topic_to_bronze(spark: SparkSession, data_type: str, logger=None):
    """
    Kafka 토픽에서 데이터를 읽어 정제 후, MERGE 방식으로 Bronze Layer에 저장
    """
    # logger가 없으면 기본 Python logger 사용
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    try:
        # 변수 설정
        kafka_topic = KAFKA_TOPICS[data_type]
        minio_path = MINIO_PATHS[data_type]
        checkpoint_path = (
            f"s3a://warehouse/checkpoints/bronze/{data_type}"  # 영구 저장소 사용
        )
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

        logger.info(
            f"[{data_type.upper()}] [STARTING] Streaming batch job for topic: {kafka_topic}"
        )

        # 1. readStream으로 Kafka 데이터 읽기
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        # 2. 각 배치에 적용할 함수 정의
        def process_micro_batch(df: DataFrame, epoch_id: int):
            logger.info(f"[{data_type.upper()}] Processing micro-batch {epoch_id}...")
            if df.isEmpty():
                logger.info(f"[{data_type.upper()}] Micro-batch is empty.")
                return

            record_count = df.count()
            logger.info(
                f"[{data_type.upper()}] [PROCESS] Read {record_count:,} new records from Kafka."
            )

            config = DATA_TYPE_CONFIG[data_type]
            merge_key_name = config["merge_key"]
            pk_col_index = config["pk_index"]

            parsed_df = df.select(
                from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("data")
            ).select("data.*")

            df_with_keys = parsed_df.withColumn(
                merge_key_name, F.trim(col("bronze_data").getItem(pk_col_index))
            ).withColumn(
                "processed_at",
                to_timestamp(col("extracted_time"), "yyyy-MM-dd HH:mm:ss"),
            )

            df_validated = df_with_keys.filter(
                F.col(merge_key_name).isNotNull() & (F.col(merge_key_name) != "")
            )

            validated_count = df_validated.count()
            dropped_count = record_count - validated_count
            if dropped_count > 0:
                logger.warning(
                    f"[{data_type.upper()}] [WARN] Dropped {dropped_count} records due to NULL/EMPTY key."
                )

            if validated_count > 0:
                write_to_delta_lake(
                    df=df_validated,
                    delta_path=minio_path,
                    table_name=f"Bronze {data_type}",
                    partition_col="processed_at",
                    merge_key=merge_key_name,
                )

        # 3. writeStream 실행
        query = (
            kafka_df.writeStream.foreachBatch(process_micro_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(once=True)
            .start()
        )
        query.awaitTermination()
        logger.info(f"[{data_type.upper()}] [SUCCESS] Streaming batch job complete.")

    except Exception as e:
        logger.error(f"Failed to process {data_type} data: {e}")
        raise e  # 에러를 다시 발생시켜 Airflow가 실패로 인지하도록 함


def main():
    """
    GDELT 3-Way Bronze Consumer 메인 함수
    """
    spark = get_spark_session("GDELT_Bronze_Consumer")

    # 스파크의 log4j 로거를 사용
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("GDELT_BRONZE_CONSUMER")

    logger.info("========== Starting GDELT 3-Way Bronze Consumer ==========")

    try:
        for data_type in ["events", "mentions", "gkg"]:

            # process_kafka_topic_to_bronze 함수에 logger를 넘겨준다
            process_kafka_topic_to_bronze(spark, data_type, logger)
        logger.info(
            "========== GDELT 3-Way Bronze Consumer FINISHED SUCCESSFULLY =========="
        )
    except Exception as e:
        logger.error(f"!!! GDELT 3-Way Bronze Consumer FAILED: {e} !!!")
    finally:
        logger.info("Spark session closed")
        spark.stop()


if __name__ == "__main__":
    main()
