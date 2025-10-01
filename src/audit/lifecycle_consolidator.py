import logging
import os
import sys
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client

# ==============================================================================
# 1. 설정 (Setup)
# ==============================================================================
# 로거 설정
logger = logging.getLogger(__name__)

# Lifecycle 테이블의 스키마를 명시적으로 정의
LIFECYCLE_SCHEMA = StructType(
    [
        StructField("global_event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField(
            "audit",
            StructType(
                [
                    StructField("bronze_arrival_time", TimestampType(), False),
                    StructField("silver_processing_end_time", TimestampType(), True),
                    StructField("gold_processing_end_time", TimestampType(), True),
                    StructField("postgres_migration_end_time", TimestampType(), True),
                ]
            ),
            False,
        ),
        StructField("status", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("hour", IntegerType(), False),
    ]
)


# ==============================================================================
# 2. 헬퍼 함수 (Helper Functions)
# ==============================================================================


def load_staging_table_safely(spark: SparkSession, path: str):
    """
    Staging 테이블을 안전하게 읽어오는 함수.
    테이블 경로가 없으면, 스키마를 지정하여 빈 데이터프레임을 생성.
    """
    try:
        if DeltaTable.isDeltaTable(spark, path):
            return spark.read.format("delta").load(path)
        else:
            logger.warning(
                f"Staging path {path} does not exist or is not a Delta table. Creating an empty DataFrame."
            )
            return spark.createDataFrame([], LIFECYCLE_SCHEMA)
    except Exception as e:
        logger.error(
            f"Unexpected error loading staging table at {path}: {e}", exc_info=True
        )
        return spark.createDataFrame([], LIFECYCLE_SCHEMA)


# ==============================================================================
# 3. 메인 로직 (Main Logic)
# ==============================================================================
def ensure_main_table_exists(spark: SparkSession, main_path: str):
    """Main lifecycle 테이블이 없으면 빈 테이블로 생성"""
    if not DeltaTable.isDeltaTable(spark, main_path):
        logger.info(
            f"Main lifecycle table not found at {main_path}. Creating empty table..."
        )
        empty_df = spark.createDataFrame([], LIFECYCLE_SCHEMA)
        empty_df.write.format("delta").partitionBy(
            "year", "month", "day", "hour", "event_type"
        ).save(main_path)
        logger.info(f"Main lifecycle table created at {main_path}")
    else:
        logger.info(f"Main lifecycle table already exists at {main_path}")


def main():
    """
    Staging lifecycle 테이블들을 Main lifecycle 테이블로 통합하는 Spark 배치 잡
    """
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark = get_spark_session("Lifecycle_Consolidator", spark_master)

    # Redis에 Spark Driver UI 정보 등록
    redis_client.register_driver_ui(spark, "Lifecycle Consolidator")

    logger.info("Lifecycle Consolidation Spark Job started.")

    main_path = "s3a://warehouse/audit/lifecycle"
    event_staging_path = "s3a://warehouse/audit/lifecycle_staging_event"
    gkg_staging_path = "s3a://warehouse/audit/lifecycle_staging_gkg"
    source_df = None

    try:
        # 0. Main 테이블 먼저 생성/확인 (가장 중요!)
        ensure_main_table_exists(spark, main_path)
        events_df = load_staging_table_safely(spark, event_staging_path)
        gkg_df = load_staging_table_safely(spark, gkg_staging_path)

        source_df = events_df.unionByName(gkg_df)
        source_df.cache()
        record_count = source_df.count()

        if record_count == 0:
            logger.info("No new data in staging tables to process. Job finished.")
            return

        logger.info(f"Found {record_count} total records to merge from staging tables.")

        # Main 테이블에 MERGE (이제 테이블이 존재한다고 확신)
        main_delta_table = DeltaTable.forPath(spark, main_path)
        main_delta_table.alias("target").merge(
            source=source_df.alias("source"),
            condition="target.global_event_id = source.global_event_id AND target.event_type = source.event_type",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info("Merge operation completed successfully.")

        logger.info("Cleaning up staging tables...")
        if not events_df.isEmpty():
            spark.sql(f"DELETE FROM delta.`{event_staging_path}`")
            logger.info("EVENT staging table cleaned up.")
        if not gkg_df.isEmpty():
            spark.sql(f"DELETE FROM delta.`{gkg_staging_path}`")
            logger.info("GKG staging table cleaned up.")

    except Exception as e:
        logger.error(f"Lifecycle Consolidation FAILED: {str(e)}", exc_info=True)
        raise e
    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        if source_df and not source_df.isEmpty():
            source_df.unpersist()
        logger.info("Lifecycle Consolidation Spark Job finished.")
        spark.stop()


# ==============================================================================
# 4. 실행 (Execution)
# ==============================================================================
if __name__ == "__main__":
    main()
