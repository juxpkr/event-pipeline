"""
GDELT 3-Way Bronze Data Consumer
- Kafka에서 3개 토픽(events, mentions, gkg)을 병렬로 처리
- 데이터 파싱, 정제, 키 추출 (공백, null 처리)
- 파티셔닝 및 MERGE를 통해 멱등성 있게 MinIO Bronze Layer에 저장
- Delta Lake 형식 사용
- 부분 실패 시 전체 롤백으로 원자성 보장
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


def setup_streaming_query(spark: SparkSession, data_type: str, logger):
    """
    지정된 데이터 타입에 대한 스트리밍 쿼리를 '설정'하고 '시작'만 시킨다.
    (awaitTermination은 호출하지 않음)
    """
    # 변수 설정
    kafka_topic = KAFKA_TOPICS[data_type]
    minio_path = MINIO_PATHS[data_type]
    checkpoint_path = (
        f"s3a://warehouse/checkpoints/bronze/{data_type}"  # 영구 저장소 사용
    )
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Lifecycle tracker 초기화
    lifecycle_tracker = EventLifecycleTracker(spark)

    # MERGE를 시도하기 전에, 테이블이 존재하는지 먼저 확인하고, 없으면 생성
    table_path = "s3a://warehouse/audit/event_lifecycle"
    try:
        spark.catalog.tableExists(f"delta.`{table_path}`")
        spark.read.format("delta").load(table_path).limit(1).collect()
    except:
        print("Lifecycle table not found. Initializing...")
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
        events/gkg만 lifecycle 추적하여 동시성 충돌을 회피.
        """
        logger.info(f"--- Starting Batch {epoch_id} for {data_type} ---")

        try:
            # 빈 배치는 즉시 종료
            if df.isEmpty():
                logger.info(f"Batch {epoch_id} is empty. Skipping.")
                return

            # 후속 작업을 위해 캐싱
            df.cache()
            record_count = df.count()

            logger.info(
                f"Processing {record_count} records for '{data_type}' in batch {epoch_id}."
            )

            # Kafka 메시지 파싱
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
                logger.warn(f"Dropped {dropped_count} records due to NULL/EMPTY key.")

            if validated_count > 0:
                # Bronze 저장
                write_to_delta_lake(
                    df=df_validated,
                    delta_path=minio_path,
                    table_name=f"Bronze {data_type}",
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
                            batch_id=f"{data_type}_batch_{epoch_id}",
                            event_type=event_type_for_tracker,
                        )
                        logger.info(
                            f"Successfully tracked {tracked_count} events in lifecycle for batch {epoch_id}."
                        )
                    except Exception as e:
                        logger.error(
                            f"LIFECYCLE TRACKING FAILED for batch {epoch_id}: {e}"
                        )
                        raise
                else:
                    logger.info(
                        f"Skipping lifecycle tracking for '{data_type}' to prevent race conditions."
                    )

        except Exception as e:
            logger.error(
                f"!!! CRITICAL ERROR in batch {epoch_id} for {data_type}: {str(e)} !!!"
            )
            raise e

        finally:
            # 캐시 해제
            df.unpersist()
            # 이 배치가 몇 개의 레코드를 처리했는지 함께 로깅
            logger.info(
                f"--- Finished Batch {epoch_id} for {data_type}, Processed {record_count} records ---"
            )

    # writeStream 실행
    query = (
        kafka_df.writeStream.foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start()
    )
    logger.info(f"[{data_type.upper()}] [SUCCESS] Streaming batch job complete.")

    return query


def main():
    """
    GDELT 3-Way Bronze Consumer 메인 함수 - 병렬로 스트림 처리
    """
    spark = get_spark_session("GDELT_Bronze_Consumer")

    # 스파크의 log4j 로거를 사용
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("GDELT_BRONZE_CONSUMER")

    logger.info("========== Starting GDELT 3-Way Bronze Consumer ==========")
    queries = []
    try:
        # STEP 1: 세 개의 스트림을 모두 시작만 시킴
        for data_type in ["events", "mentions", "gkg"]:
            logger.info(f"Setting up stream for data type: {data_type.upper()}")

            # readStream과 writeStream을 분리해서, writeStream 쿼리 객체만 반환하도록 변경
            query = setup_streaming_query(spark, data_type, logger)
            queries.append(query)

        # STEP 2: 모든 스트림이 끝날 때까지 감시하고, 중간에 하나라도 실패하면 모두 중지시킨다.
        # 모든 스트리밍 쿼리가 끝날 때까지 대기
        # Spark 3.x 이상에서는 spark.streams.awaitAnyTermination() 사용 가능
        # 이 코드는 Airflow에서 SparkSubmitOperator로 실행되므로,
        # trigger(once=True)와 함께라면 각 배치가 끝나면 자동으로 종료됨.
        # 따라서 awaitTermination()을 모든 쿼리에 대해 호출해주면 됨.

        # 부분 실패 감지를 위한 상태 체크 루프
        while any([q.isActive for q in queries]):
            # 0.5초마다 각 쿼리의 상태를 체크
            time.sleep(0.5)

        for query in queries:
            if query.exception():
                failed_query_name = query.name if query.name else "Unknown Query"
                error_message = str(query.exception())
                logger.error(
                    f"!!! CRITICAL FAILURE DETECTED in query '{failed_query_name}' !!!"
                )
                logger.error(f"!!! Error: {error_message} !!!")

                # 실패가 감지되면, 아직 실행 중인 다른 모든 쿼리를 즉시 중단시킨다
                logger.warn("Stopping all other active queries to ensure atomicity...")
                for q in queries:
                    if q.isActive:
                        q.stop()

                # 에러를 발생시켜 전체 실패
                raise query.exception()

        logger.info(
            "========== GDELT 3-Way Bronze Consumer FINISHED SUCCESSFULLY =========="
        )
    except Exception as e:
        logger.error(f"!!! GDELT 3-Way Bronze Consumer FAILED: {e} !!!")
        raise e
    finally:
        logger.info("Spark session closed")
        spark.stop()


if __name__ == "__main__":
    main()
