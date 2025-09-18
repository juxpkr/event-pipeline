"""GDELT 3-Way Silver Processor

Bronze Layer에서 수집된 원본 데이터를 정제하고, 가공하여 Gold Layer에서 분석하기 좋은 형태로 변환
주요 기능으로는 데이터 타입 변환, null 값 처리, 칼럼 정규화, 3-Way 조인
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from datetime import datetime


# 모듈 임포트
sys.path.append("/opt/airflow")
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from src.utils.schemas.gdelt_schemas import GDELTSchemas

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
    spark: SparkSession, table_name: str, silver_path: str, schema: StructType, partition_keys: list
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

        # NOT NULL 컬럼은 타입별 기본값
        type_defaults = {
            (IntegerType, LongType, DoubleType, FloatType): 0,
            StringType: "",
            (TimestampType, DateType): datetime(1900, 1, 1),
            BooleanType: False
        }

        for type_group, default_val in type_defaults.items():
            if isinstance(col_type, type_group):
                return default_val
        return None  # 알 수 없는 타입

    dummy_row_dict = {field.name: get_dummy_value(field) for field in schema.fields}
    
    dummy_row = [tuple(dummy_row_dict.get(field.name) for field in schema.fields)]
    dummy_df = spark.createDataFrame(dummy_row, schema)

    (
        dummy_df.write.format("delta")
        .mode("overwrite") # ignore 대신 overwrite로 해서 확실하게 생성
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
    """MinIO Bronze Layer에서 새로 추가된 데이터만!! 읽어 DataFrame으로 반환"""
    logger.info("Reading Bronze data from MinIO...")

    # Bronze Layer 경로 설정
    bronze_paths = {
        "events": "s3a://warehouse/bronze/gdelt_events",
        "mentions": "s3a://warehouse/bronze/gdelt_mentions",
        "gkg": "s3a://warehouse/bronze/gdelt_gkg",
    }

    all_dataframes = []

    for data_type, base_path in bronze_paths.items():
        try:
            logger.info(f"Reading {data_type.upper()} data from {base_path}")

            # Delta 테이블에서 읽기 (최신 파티션만)
            bronze_df = (
                spark.read.format("delta")
                .load(base_path)
                .filter(
                    F.col("processed_at") == F.lit(start_time_str).cast("timestamp")
                )
            )

            if not bronze_df.rdd.isEmpty():
                # data_type 컬럼 추가
                bronze_with_type = bronze_df.withColumn("data_type", F.lit(data_type))
                all_dataframes.append(bronze_with_type)

                record_count = bronze_df.count()
                logger.info(
                    f"Loaded {record_count:,} {data_type.upper()} records from Bronze"
                )
            else:
                logger.warning(f"No data found for {data_type}")

        except Exception as e:
            logger.error(f"Failed to read {data_type} from Bronze: {e}")
            continue

    if not all_dataframes:
        logger.error("No Bronze data found in any data type")
        return spark.createDataFrame([], StructType([]))

    # 모든 데이터 타입을 하나의 DataFrame으로 합치기
    combined_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        combined_df = combined_df.union(df)

    total_records = combined_df.count()
    logger.info(f"Total Bronze records loaded: {total_records:,}")

    return combined_df


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

    try:
        # 1. Silver 테이블 설정
        logger.info("Setting up Silver tables...")
        setup_silver_table(
           spark,
           "default.gdelt_events",
           "s3a://warehouse/silver/gdelt_events",
           GDELTSchemas.get_silver_events_schema(),
           partition_keys=["year", "month", "day", "hour"]
        )
        setup_silver_table(
           spark,
           "default.gdelt_events_detailed",
           "s3a://warehouse/silver/gdelt_events_detailed",
           GDELTSchemas.get_silver_events_detailed_schema(),
           partition_keys=["year", "month", "day", "hour"]
        )

        # 2. MinIO Bronze Layer에서 데이터 읽기 (Airflow가 준 시간 인자 전달)
        parsed_df = read_from_bronze_minio(spark, start_time_str, end_time_str)

        if parsed_df.rdd.isEmpty():
            logger.warning("No Bronze data found in MinIO. Exiting gracefully.")
            return

        # 3. 데이터 타입별 분리 및 변환
        events_df = parsed_df.filter(F.col("data_type") == "events")
        mentions_df = parsed_df.filter(F.col("data_type") == "mentions")
        gkg_df = parsed_df.filter(F.col("data_type") == "gkg")

        # 4. 각 데이터 타입 변환
        events_silver = (
            transform_events_to_silver(events_df)
            if not events_df.rdd.isEmpty()
            else None
        )
        mentions_silver = (
            transform_mentions_to_silver(mentions_df)
            if not mentions_df.rdd.isEmpty()
            else None
        )
        gkg_silver = (
            transform_gkg_to_silver(gkg_df) if not gkg_df.rdd.isEmpty() else None
        )

        # 5. Events 단독 Silver 저장
        if events_silver:
            write_to_delta_lake(
                df=events_silver,
                delta_path="s3a://warehouse/silver/gdelt_events",
                table_name="Events",
                partition_col="processed_at",  # Hot: 실시간 대시보드용 (수집시간)
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
            )
            logger.info("Events Silver sample:")
            events_silver.select(
                "global_event_id", "event_date", "actor1_country_code", "event_code"
            ).show(3)

        # 6. 3-Way 조인 수행
        logger.info("Performing 3-Way Join...")
        joined_df = perform_three_way_join(events_silver, mentions_silver, gkg_silver)

        # 조인 결과가 있을 때만 후속 처리 진행
        if not joined_df.rdd.isEmpty():
            # 7. 우선순위 날짜 생성
            joined_with_priority = create_priority_date(joined_df)

            # 8. 최종 컬럼 선택
            final_silver_df = select_final_columns(joined_with_priority)

            # 9. Events detailed Silver 저장
            write_to_delta_lake(
                df=final_silver_df,
                delta_path="s3a://warehouse/silver/gdelt_events_detailed",
                table_name="Events detailed GDELT",
                partition_col="priority_date",  # 사건 시간 기준
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
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
