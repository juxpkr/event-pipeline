"""
일일 GDELT 데이터 처리기 (마이크로 배치용)
- Bronze → Silver 변환
- 기존 gdelt_silver_processor 로직 재사용
- 하루치만 처리하므로 메모리 안정적
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from src.utils.schemas.gdelt_schemas import GDELTSchemas
from src.audit.lifecycle_updater import EventLifecycleUpdater

# 기존 변환 로직 재사용
from src.processing.transformers.events_transformer import transform_events_to_silver
from src.processing.transformers.mentions_transformer import transform_mentions_to_silver
from src.processing.transformers.gkg_transformer import transform_gkg_to_silver
from src.processing.joiners.gdelt_three_way_joiner import perform_three_way_join, select_final_columns
from src.processing.partitioning.gdelt_date_priority import create_priority_date
from src.processing.partitioning.gdelt_partition_writer import write_to_delta_lake

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def read_daily_bronze_data(spark: SparkSession, date_str: str) -> dict:
    """하루치 Bronze 데이터 읽기"""
    logger.info(f"Reading Bronze data for {date_str}")

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day

    bronze_paths = {
        "events": "s3a://warehouse/bronze/gdelt_events",
        "mentions": "s3a://warehouse/bronze/gdelt_mentions",
        "gkg": "s3a://warehouse/bronze/gdelt_gkg",
    }

    dataframes = {}

    for data_type, path in bronze_paths.items():
        try:
            # 해당 날짜의 파티션만 읽기
            df = spark.read.format("delta").load(path) \
                .filter(
                    (F.col("year") == year) &
                    (F.col("month") == month) &
                    (F.col("day") == day)
                )

            if df.first():  # 데이터 존재 확인
                dataframes[data_type] = df
                logger.info(f"Loaded {data_type} data for {date_str}")
            else:
                logger.warning(f"No {data_type} data found for {date_str}")

        except Exception as e:
            logger.error(f"Failed to read {data_type} Bronze data: {e}")

    return dataframes

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


def setup_silver_tables_legacy(spark: SparkSession):
    """Silver 테이블 구조 설정 (필요시에만)"""
    # Silver 스키마 생성
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver LOCATION 's3a://warehouse/silver/'")

    # 테이블 존재 확인 및 생성
    tables_to_create = [
        ("silver.gdelt_events", "s3a://warehouse/silver/gdelt_events", GDELTSchemas.get_silver_events_schema()),
        ("silver.gdelt_events_detailed", "s3a://warehouse/silver/gdelt_events_detailed", GDELTSchemas.get_silver_events_detailed_schema())
    ]

    for table_name, table_path, schema in tables_to_create:
        if not spark.catalog.tableExists(table_name):
            logger.info(f"Creating table structure for {table_name}")

            # 더미 데이터로 테이블 구조 생성
            dummy_data = [(None,) * len(schema.fields)]
            dummy_df = spark.createDataFrame(dummy_data, schema)

            dummy_df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("year", "month", "day", "hour") \
                .saveAsTable(table_name, path=table_path)

            # 더미 데이터 삭제
            spark.sql(f"DELETE FROM delta.`{table_path}` WHERE 1=1")
            logger.info(f"Table {table_name} structure created")

def process_daily_data(date_str: str):
    """하루치 데이터 처리 (기존 gdelt_silver_processor 로직 완전 이식)"""
    logger.info(f"Starting daily processing for {date_str}")

    spark = get_spark_session(f"Daily_GDELT_Processor_{date_str}", "spark://spark-master:7077")
    redis_client.register_driver_ui(spark, f"Daily Processor {date_str}")

    # batch_id 생성 (날짜 기반)
    batch_id = f"daily_batch_{date_str.replace('-', '_')}"

    try:
        # Lifecycle Tracker 초기화
        lifecycle_updater = EventLifecycleUpdater(spark)

        # 1. Silver 스키마 생성
        logger.info("Creating silver schema...")
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver LOCATION 's3a://warehouse/silver/'")

        # 2. Silver 테이블 설정 (기존 로직 재사용)
        logger.info("Setting up Silver tables...")
        setup_silver_table(
            spark,
            "silver.gdelt_events",
            "s3a://warehouse/silver/gdelt_events",
            GDELTSchemas.get_silver_events_schema(),
            partition_keys=["year", "month", "day", "hour"],
        )
        setup_silver_table(
            spark,
            "silver.gdelt_events_detailed",
            "s3a://warehouse/silver/gdelt_events_detailed",
            GDELTSchemas.get_silver_events_detailed_schema(),
            partition_keys=["year", "month", "day", "hour"],
        )

        # 3. Bronze 데이터 읽기
        bronze_dataframes = read_daily_bronze_data(spark, date_str)

        if not bronze_dataframes:
            logger.warning(f"No Bronze data found for {date_str}. Exiting gracefully.")
            return

        # 4. 데이터 타입별 분리 및 변환
        events_df = bronze_dataframes.get("events")
        mentions_df = bronze_dataframes.get("mentions")
        gkg_df = bronze_dataframes.get("gkg")

        # 5. 각 데이터 타입 변환
        events_silver = transform_events_to_silver(events_df) if events_df else None
        mentions_silver = transform_mentions_to_silver(mentions_df) if mentions_df else None
        gkg_silver = transform_gkg_to_silver(gkg_df) if gkg_df else None

        # 6. Events 단독 Silver 저장
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
                delta_path="s3a://warehouse/silver/gdelt_events_detailed",
                table_name="Events detailed GDELT",
                partition_col="priority_date",  # 사건 시간 기준
                merge_key="global_event_id",  # Silver 스키마의 소문자 키
            )

            # Silver 처리 완료 시점 기록
            final_event_ids = final_silver_df.select("global_event_id").rdd.map(lambda row: row[0]).collect()
            lifecycle_updater.mark_silver_processing_complete(final_event_ids, batch_id)

            # 실제 GKG 조인 성공률 로깅
            actually_joined_count = final_silver_df.filter(F.col("gkg_record_id").isNotNull()).count()
            total_events = final_silver_df.count()
            actual_join_rate = (actually_joined_count / total_events * 100) if total_events > 0 else 0
            logger.info(f"Silver processing complete: {total_events} events processed, {actually_joined_count} with GKG ({actual_join_rate:.1f}% join rate)")

            logger.info("Sample of Events detailed Silver data:")
            final_silver_df.show(5, vertical=True)

        logger.info(f"Silver Layer processing completed successfully for {date_str}!")

    except Exception as e:
        logger.error(f"Error in daily Silver processing: {e}", exc_info=True)
        raise

    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        spark.stop()
        logger.info("Spark session closed")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: daily_gdelt_processor.py <YYYY-MM-DD>")
        sys.exit(1)

    date_str = sys.argv[1]

    # 날짜 형식 검증
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD")
        sys.exit(1)

    process_daily_data(date_str)