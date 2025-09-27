"""GDELT 3-Way Silver Processor

Bronze Layer에서 수집된 원본 데이터를 정제하고, 가공하여 Gold Layer에서 분석하기 좋은 형태로 변환
주요 기능으로는 데이터 타입 변환, null 값 처리, 칼럼 정규화, 3-Way 조인
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone


# 모듈 임포트
sys.path.append("/opt/airflow")
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from src.utils.schemas.gdelt_schemas import GDELTSchemas
from src.audit.lifecycle_updater import EventLifecycleUpdater

# Transformers
from src.processing.transformers.events_transformer import transform_events_to_silver
from src.processing.transformers.mentions_transformer import (
    transform_mentions_to_silver,
)
from src.processing.transformers.gkg_transformer import transform_gkg_to_silver

# Joiners
from src.processing.joiners.gdelt_three_way_joiner import (
    perform_three_way_join,
    select_final_columns,
)

# Partitioning
from src.processing.partitioning.gdelt_date_priority import create_priority_date
from src.processing.partitioning.gdelt_partition_writer import write_to_delta_lake

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_silver_table(
    spark: SparkSession,
    table_name: str,
    silver_path: str,
    schema: StructType,
    partition_keys: list,
):
    """(최초 1회만) 더미 데이터를 이용해 파티션 구조를 가진 Hive 테이블을 생성"""
    if spark.catalog.tableExists(table_name):
        logger.info(f"Table '{table_name}' already exists. Skipping setup.")
        return

    logger.info(f"Table '{table_name}' not found. Creating with dummy data...")

    """Silver 테이블 구조를 미리 생성"""
    db_name = table_name.split(".")[0]
    logger.info(f"Creating database '{db_name}' and table '{table_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    # 스키마에 맞는 더미 데이터를 동적으로 생성
    def get_dummy_value(field):
        """필드 타입과 제약조건에 따른 더미 값 생성"""
        col_name, col_type, nullable = field.name, field.dataType, field.nullable

        # 파티션 컬럼은 고정 더미 값 (명백한 더미 값 사용)
        partition_values = {"year": 9999, "month": 99, "day": 99, "hour": 99}
        if col_name in partition_values:
            return partition_values[col_name]

        # nullable 컬럼은 None
        if nullable:
            return None

        # if/else로 타입별 기본값 지정
        if isinstance(col_type, (IntegerType, LongType, DoubleType, FloatType)):
            return 0
        elif isinstance(col_type, StringType):
            return ""
        elif isinstance(col_type, (TimestampType, DateType)):
            # UTC 시간대로 명시
            return datetime(1900, 1, 1, tzinfo=timezone.utc)
        elif isinstance(col_type, BooleanType):
            return False
        else:
            # 알 수 없는 타입에 대한 처리
            logger.warning(
                f"Unknown data type for column '{col_name}': {col_type}. Defaulting to None."
            )
            return None

    dummy_row_dict = {field.name: get_dummy_value(field) for field in schema.fields}

    dummy_row = [tuple(dummy_row_dict.get(field.name) for field in schema.fields)]
    dummy_df = spark.createDataFrame(dummy_row, schema)

    (
        dummy_df.write.format("delta")
        .mode("overwrite")  # ignore 대신 overwrite로 해서 확실하게 생성
        .option("overwriteSchema", "true")
        .partitionBy(*partition_keys)
        .saveAsTable(table_name, path=silver_path)
    )
    # 더미 데이터 삭제 (SQL 방식)
    spark.sql(f"DELETE FROM delta.`{silver_path}` WHERE year = 9999")
    logger.info(f"Table '{table_name}' structure created successfully.")


def read_from_bronze_minio(
    spark: SparkSession, start_time_str: str, end_time_str: str
) -> DataFrame:
    """
    MinIO Bronze Layer에서 각 데이터 타입별로 데이터를 읽고,
    DataFrame의 딕셔너리 형태로 반환
    """
    logger.info(
        f"Reading Bronze data from MinIO for period: {start_time_str} to {end_time_str}"
    )
    # 백필 전용 Bronze Layer 경로 설정
    bronze_paths = {
        "events": "s3a://warehouse/bronze_backfill/gdelt_events",
        "mentions": "s3a://warehouse/bronze_backfill/gdelt_mentions",
        "gkg": "s3a://warehouse/bronze_backfill/gdelt_gkg",
    }

    # 반환 타입을 딕셔너리로 변경 (기존: 통합 dataframe)
    dataframes = {}

    for data_type, base_path in bronze_paths.items():
        try:
            logger.info(f"Reading {data_type.upper()} data from {base_path}")

            # Delta 테이블에서 읽기 (최신 파티션만)
            bronze_df = (
                spark.read.format("delta")
                .load(base_path)
                .filter(
                    (F.col("processed_at") >= F.lit(start_time_str).cast("timestamp"))
                    & (F.col("processed_at") < F.lit(end_time_str).cast("timestamp"))
                )
            )

            # count() 대신 first()로 존재 여부만 빠르게 확인하기
            if bronze_df.first():
                logger.info(f"Successfully loaded data for {data_type.upper()}.")
                dataframes[data_type] = bronze_df
            else:
                logger.warning(
                    f"No new data found for {data_type.upper()} in the given time range."
                )

        except Exception as e:
            logger.error(f"Failed to read {data_type} from Bronze: {e}")
            continue  # 하나의 타입이 실패해도 다른 타입은 계속 시도

    return dataframes


def main():
    """메인 실행 함수"""
    logger.info("Starting GDELT 3-Way Silver Processor (Refactored)...")

    spark = get_spark_session(
        "GDELT 3Way Silver Processor", "spark://spark-master:7077"
    )
    redis_client.register_driver_ui(spark, "GDELT 3Way Silver Processor")

    # Airflow가 넘겨준 인자를 sys.argv를 통해 받음
    if len(sys.argv) != 3:
        logger.error("Usage: spark-submit <script> <start_time> <end_time>")
        sys.exit(1)

    start_time_str = sys.argv[1]
    end_time_str = sys.argv[2]

    # batch_id 생성 (시간 기반)
    batch_id = (
        f"batch_{start_time_str.replace(':', '').replace('-', '').replace(' ', '_')}"
    )

    try:
        # Lifecycle Tracker 초기화
        lifecycle_updater = EventLifecycleUpdater(spark)
        # 1. Silver 스키마 생성
        logger.info("Creating silver schema...")
        spark.sql(
            "CREATE SCHEMA IF NOT EXISTS silver_backfill LOCATION 's3a://warehouse/silver_backfill/'"
        )

        # 2. Silver 테이블 설정
        logger.info("Setting up Silver tables...")
        setup_silver_table(
            spark,
            "silver_backfill.gdelt_events",
            "s3a://warehouse/silver_backfill/gdelt_events",
            GDELTSchemas.get_silver_events_schema(),
            partition_keys=["year", "month", "day", "hour"],
        )
        setup_silver_table(
            spark,
            "silver_backfill.gdelt_events_detailed",
            "s3a://warehouse/silver_backfill/gdelt_events_detailed",
            GDELTSchemas.get_silver_events_detailed_schema(),
            partition_keys=["year", "month", "day", "hour"],
        )

        # 3. MinIO Bronze Layer에서 데이터 읽기 (Airflow가 준 시간 인자 전달)
        bronze_dataframes = read_from_bronze_minio(spark, start_time_str, end_time_str)

        if not bronze_dataframes:
            logger.warning("No Bronze data found in MinIO. Exiting gracefully.")
            return

        # 4. 데이터 타입별 분리 및 변환 (이제 filter가 필요 없음!)
        events_df = bronze_dataframes.get("events")
        mentions_df = bronze_dataframes.get("mentions")
        gkg_df = bronze_dataframes.get("gkg")

        # 5. 각 데이터 타입 변환 (if df 로 None과 Enpty를 동시에 체크할 수 있음)
        events_silver = transform_events_to_silver(events_df) if events_df else None
        mentions_silver = (
            transform_mentions_to_silver(mentions_df) if mentions_df else None
        )
        gkg_silver = transform_gkg_to_silver(gkg_df) if gkg_df else None

        # 6. Events 단독 Silver 저장
        if events_silver:
            write_to_delta_lake(
                df=events_silver,
                delta_path="s3a://warehouse/silver_backfill/gdelt_events",
                table_name="Events",
                partition_col="processed_at",  # Hot: 실시간 대시보드용 (수집시간)
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
            )

            logger.info("Events Silver sample:")
            events_silver.select(
                "global_event_id", "event_date", "actor1_country_code", "event_code"
            ).show(3)

        # 7. 3-Way 조인 수행
        logger.info("Performing 3-Way Join...")
        joined_df = perform_three_way_join(events_silver, mentions_silver, gkg_silver)

        # 조인 결과가 있을 때만 후속 처리 진행
        if joined_df and not joined_df.rdd.isEmpty():  # 조인 후에는 isEmpty() 체크 필요
            # 8. 우선순위 날짜 생성
            joined_with_priority = create_priority_date(joined_df)

            # 9. 최종 컬럼 선택
            final_silver_df = select_final_columns(joined_with_priority)

            # 10. Events detailed Silver 저장
            write_to_delta_lake(
                df=final_silver_df,
                delta_path="s3a://warehouse/silver_backfill/gdelt_events_detailed",
                table_name="Events detailed GDELT",
                partition_col="priority_date",  # 사건 시간 기준
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
            )

            # Silver 처리 완료 시점 기록
            final_event_ids = (
                final_silver_df.select("global_event_id")
                .rdd.map(lambda row: row[0])
                .collect()
            )
            lifecycle_updater.mark_silver_processing_complete(final_event_ids, batch_id)

            # 실제 GKG 조인 성공률 로깅
            actually_joined_count = final_silver_df.filter(
                F.col("gkg_record_id").isNotNull()
            ).count()
            total_events = final_silver_df.count()
            actual_join_rate = (
                (actually_joined_count / total_events * 100) if total_events > 0 else 0
            )
            logger.info(
                f"Silver processing complete: {total_events} events processed, {actually_joined_count} with GKG ({actual_join_rate:.1f}% join rate)"
            )

            logger.info("Sample of Events detailed Silver data:")
            final_silver_df.show(5, vertical=True)

        logger.info("Silver Layer processing completed successfully!")

    except Exception as e:
        logger.error(f"Error in 3-Way Silver processing: {e}", exc_info=True)

    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    main()
