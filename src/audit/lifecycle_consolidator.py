import logging
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 로거 설정
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    """공용 스파크 세션 빌더"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    """
    Staging lifecycle 테이블들을 Main lifecycle 테이블로 통합하는 Spark 배치 잡
    """
    spark = get_spark_session("Lifecycle_Consolidator")
    logger.info("Lifecycle Consolidation Spark Job started.")

    # 경로 정의
    main_path = "s3a://warehouse/audit/event_lifecycle"
    event_staging_path = "s3a://warehouse/audit/lifecycle_staging_event"
    gkg_staging_path = "s3a://warehouse/audit/lifecycle_staging_gkg"

    try:
        # 1. 두 Staging 테이블 읽기 (테이블 존재 여부 확인)
        logger.info(
            f"Reading from staging tables: {event_staging_path}, {gkg_staging_path}"
        )

        # EVENT staging 테이블 읽기
        if DeltaTable.isDeltaTable(spark, event_staging_path):
            events_df = spark.read.format("delta").load(event_staging_path)
        else:
            logger.info("No EVENT staging data found")
            events_df = spark.createDataFrame([], None)  # 빈 DataFrame

        # GKG staging 테이블 읽기
        if DeltaTable.isDeltaTable(spark, gkg_staging_path):
            gkg_df = spark.read.format("delta").load(gkg_staging_path)
        else:
            logger.info("No GKG staging data found")
            gkg_df = spark.createDataFrame([], None)  # 빈 DataFrame

        # 2. 하나의 데이터프레임으로 통합
        if events_df.count() == 0 and gkg_df.count() == 0:
            logger.info("No new data in staging tables. Job finished.")
            spark.stop()
            return

        # 빈 DataFrame 필터링하여 union
        dataframes_to_union = []
        if events_df.count() > 0:
            dataframes_to_union.append(events_df)
        if gkg_df.count() > 0:
            dataframes_to_union.append(gkg_df)

        if len(dataframes_to_union) == 1:
            source_df = dataframes_to_union[0]
        else:
            source_df = dataframes_to_union[0].unionByName(dataframes_to_union[1])
        source_df.cache()
        record_count = source_df.count()
        logger.info(f"Found {record_count} total records to merge from staging tables.")

        # 3. Main Lifecycle 테이블에 MERGE
        logger.info(f"Merging {record_count} records into main table: {main_path}")

        # Main 테이블이 없으면 생성, 있으면 열기
        if not DeltaTable.isDeltaTable(spark, main_path):
            logger.info(
                f"Main table not found at {main_path}. Creating it with the first batch."
            )
            source_df.write.format("delta").partitionBy(
                "year", "month", "day", "hour", "event_type"
            ).save(main_path)
        else:
            main_delta_table = DeltaTable.forPath(spark, main_path)
            main_delta_table.alias("target").merge(
                source=source_df.alias("source"),
                condition="target.global_event_id = source.global_event_id AND target.event_type = source.event_type",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info("Merge operation completed successfully.")

        # 4. Staging 테이블 데이터 정리 (성공 시에만)
        logger.info("Cleaning up staging tables...")

        # EVENT staging 정리
        if DeltaTable.isDeltaTable(spark, event_staging_path):
            spark.sql(f"DELETE FROM delta.`{event_staging_path}`")
            logger.info("EVENT staging table cleaned up")

        # GKG staging 정리
        if DeltaTable.isDeltaTable(spark, gkg_staging_path):
            spark.sql(f"DELETE FROM delta.`{gkg_staging_path}`")
            logger.info("GKG staging table cleaned up")

    except Exception as e:
        logger.error(f"Lifecycle Consolidation FAILED: {str(e)}", exc_info=True)
        raise e
    finally:
        try:
            source_df.unpersist()
        except:
            pass  # source_df가 정의되지 않았을 수도 있음
        logger.info("Lifecycle Consolidation Spark Job finished.")
        spark.stop()


if __name__ == "__main__":
    main()
