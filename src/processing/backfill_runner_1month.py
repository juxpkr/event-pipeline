"""
GDELT 1개월 백필 러너 - 지정된 기간 동안 BigQuery에서 히스토리컬 데이터를 가져와 Silver Layer로 처리
"""

import os
import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
import logging
from datetime import datetime, timedelta

def get_gdelt_silver_schema():
    """GDELT Silver Table 스키마 정의 (gdelt_silver_processor와 동일)"""
    return StructType([
        # 기본 식별자 (0-4)
        StructField("global_event_id", LongType(), True),
        StructField("day", IntegerType(), True),
        StructField("month_year", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("fraction_date", DoubleType(), True),
        # 주체(Actor1) 정보 (5-14)
        StructField("actor1_code", StringType(), True),
        StructField("actor1_name", StringType(), True),
        StructField("actor1_country_code", StringType(), True),
        StructField("actor1_known_group_code", StringType(), True),
        StructField("actor1_ethnic_code", StringType(), True),
        StructField("actor1_religion1_code", StringType(), True),
        StructField("actor1_religion2_code", StringType(), True),
        StructField("actor1_type1_code", StringType(), True),
        StructField("actor1_type2_code", StringType(), True),
        StructField("actor1_type3_code", StringType(), True),
        # 대상(Actor2) 정보 (15-24)
        StructField("actor2_code", StringType(), True),
        StructField("actor2_name", StringType(), True),
        StructField("actor2_country_code", StringType(), True),
        StructField("actor2_known_group_code", StringType(), True),
        StructField("actor2_ethnic_code", StringType(), True),
        StructField("actor2_religion1_code", StringType(), True),
        StructField("actor2_religion2_code", StringType(), True),
        StructField("actor2_type1_code", StringType(), True),
        StructField("actor2_type2_code", StringType(), True),
        StructField("actor2_type3_code", StringType(), True),
        # 이벤트 정보 (25-34)
        StructField("is_root_event", IntegerType(), True),
        StructField("event_code", StringType(), True),
        StructField("event_base_code", StringType(), True),
        StructField("event_root_code", StringType(), True),
        StructField("quad_class", IntegerType(), True),
        StructField("goldstein_scale", DoubleType(), True),
        StructField("num_mentions", IntegerType(), True),
        StructField("num_sources", IntegerType(), True),
        StructField("num_articles", IntegerType(), True),
        StructField("avg_tone", DoubleType(), True),
        # 주체1 지리정보 (35-41)
        StructField("actor1_geo_type", StringType(), True),
        StructField("actor1_geo_fullname", StringType(), True),
        StructField("actor1_geo_country_code", StringType(), True),
        StructField("actor1_geo_adm1_code", StringType(), True),
        StructField("actor1_geo_lat", DoubleType(), True),
        StructField("actor1_geo_long", DoubleType(), True),
        StructField("actor1_geo_feature_id", StringType(), True),
        # 대상2 지리정보 (42-48)
        StructField("actor2_geo_type", StringType(), True),
        StructField("actor2_geo_fullname", StringType(), True),
        StructField("actor2_geo_country_code", StringType(), True),
        StructField("actor2_geo_adm1_code", StringType(), True),
        StructField("actor2_geo_lat", DoubleType(), True),
        StructField("actor2_geo_long", DoubleType(), True),
        StructField("actor2_geo_feature_id", StringType(), True),
        # 사건 지리정보 (49-55)
        StructField("action_geo_type", StringType(), True),
        StructField("action_geo_fullname", StringType(), True),
        StructField("action_geo_country_code", StringType(), True),
        StructField("action_geo_adm1_code", StringType(), True),
        StructField("action_geo_lat", DoubleType(), True),
        StructField("action_geo_long", DoubleType(), True),
        StructField("action_geo_feature_id", StringType(), True),
        # 추가 정보 (56-60)
        StructField("date_added", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("actor1_geo_centroid", StringType(), True),
        StructField("actor2_geo_centroid", StringType(), True),
        StructField("action_geo_centroid", StringType(), True),
        # 메타데이터
        StructField("processed_time", TimestampType(), True),
        StructField("source_file", StringType(), True),
    ])


def read_from_kafka(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """Kafka에서 특정 기간의 Raw 데이터 읽기 (타임스탬프 기반)"""
    logging.info(f"Reading Kafka raw data from {start_date} to {end_date}")

    # 날짜를 타임스탬프로 변환 (밀리초)
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    raw_df = (
        spark.read.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        )
        .option("subscribe", "gdelt_raw_events")
        .option("startingOffsets", f"""{{\"gdelt_raw_events\":{{\"0\":{start_timestamp}}}}}""")
        .option("endingOffsets", f"""{{\"gdelt_raw_events\":{{\"0\":{end_timestamp}}}}}""")
        .load()
    )

    # Kafka 메시지 파싱 (gdelt_silver_processor와 동일)
    parsed_df = raw_df.select(
        F.from_json(
            F.col("value").cast("string"),
            StructType([
                StructField("raw_data", ArrayType(StringType()), True),
                StructField("row_number", IntegerType(), True),
                StructField("source_file", StringType(), True),
                StructField("extracted_time", StringType(), True),
                StructField("source_url", StringType(), True),
                StructField("total_columns", IntegerType(), True),
            ]),
        ).alias("data")
    ).select("data.*")

    return parsed_df


def transform_raw_to_silver(raw_df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Raw 데이터를 Silver 스키마로 변환 (gdelt_silver_processor와 동일)"""

    silver_df = raw_df.select(
        # 기본 식별자
        F.when(F.col("raw_data")[0] != "", F.col("raw_data")[0].cast(LongType())).alias("global_event_id"),
        F.when(F.col("raw_data")[1] != "", F.col("raw_data")[1].cast(IntegerType())).alias("day"),
        F.when(F.col("raw_data")[2] != "", F.col("raw_data")[2].cast(IntegerType())).alias("month_year"),
        F.when(F.col("raw_data")[3] != "", F.col("raw_data")[3].cast(IntegerType())).alias("year"),
        F.when(F.col("raw_data")[4] != "", F.col("raw_data")[4].cast(DoubleType())).alias("fraction_date"),
        # 주체(Actor1) 정보
        F.col("raw_data")[5].alias("actor1_code"),
        F.col("raw_data")[6].alias("actor1_name"),
        F.col("raw_data")[7].alias("actor1_country_code"),
        F.col("raw_data")[8].alias("actor1_known_group_code"),
        F.col("raw_data")[9].alias("actor1_ethnic_code"),
        F.col("raw_data")[10].alias("actor1_religion1_code"),
        F.col("raw_data")[11].alias("actor1_religion2_code"),
        F.col("raw_data")[12].alias("actor1_type1_code"),
        F.col("raw_data")[13].alias("actor1_type2_code"),
        F.col("raw_data")[14].alias("actor1_type3_code"),
        # 대상(Actor2) 정보
        F.col("raw_data")[15].alias("actor2_code"),
        F.col("raw_data")[16].alias("actor2_name"),
        F.col("raw_data")[17].alias("actor2_country_code"),
        F.col("raw_data")[18].alias("actor2_known_group_code"),
        F.col("raw_data")[19].alias("actor2_ethnic_code"),
        F.col("raw_data")[20].alias("actor2_religion1_code"),
        F.col("raw_data")[21].alias("actor2_religion2_code"),
        F.col("raw_data")[22].alias("actor2_type1_code"),
        F.col("raw_data")[23].alias("actor2_type2_code"),
        F.col("raw_data")[24].alias("actor2_type3_code"),
        # 이벤트 정보
        F.when(F.col("raw_data")[25] != "", F.col("raw_data")[25].cast(IntegerType())).alias("is_root_event"),
        F.col("raw_data")[26].alias("event_code"),
        F.col("raw_data")[27].alias("event_base_code"),
        F.col("raw_data")[28].alias("event_root_code"),
        F.when(F.col("raw_data")[29] != "", F.col("raw_data")[29].cast(IntegerType())).alias("quad_class"),
        F.when(F.col("raw_data")[30] != "", F.col("raw_data")[30].cast(DoubleType())).alias("goldstein_scale"),
        F.when(F.col("raw_data")[31] != "", F.col("raw_data")[31].cast(IntegerType())).alias("num_mentions"),
        F.when(F.col("raw_data")[32] != "", F.col("raw_data")[32].cast(IntegerType())).alias("num_sources"),
        F.when(F.col("raw_data")[33] != "", F.col("raw_data")[33].cast(IntegerType())).alias("num_articles"),
        F.when(F.col("raw_data")[34] != "", F.col("raw_data")[34].cast(DoubleType())).alias("avg_tone"),
        # 주체1 지리정보
        F.col("raw_data")[35].alias("actor1_geo_type"),
        F.col("raw_data")[36].alias("actor1_geo_fullname"),
        F.col("raw_data")[37].alias("actor1_geo_country_code"),
        F.col("raw_data")[38].alias("actor1_geo_adm1_code"),
        F.when(F.col("raw_data")[39] != "", F.col("raw_data")[39].cast(DoubleType())).alias("actor1_geo_lat"),
        F.when(F.col("raw_data")[40] != "", F.col("raw_data")[40].cast(DoubleType())).alias("actor1_geo_long"),
        F.col("raw_data")[41].alias("actor1_geo_feature_id"),
        # 대상2 지리정보
        F.col("raw_data")[42].alias("actor2_geo_type"),
        F.col("raw_data")[43].alias("actor2_geo_fullname"),
        F.col("raw_data")[44].alias("actor2_geo_country_code"),
        F.col("raw_data")[45].alias("actor2_geo_adm1_code"),
        F.when(F.col("raw_data")[46] != "", F.col("raw_data")[46].cast(DoubleType())).alias("actor2_geo_lat"),
        F.when(F.col("raw_data")[47] != "", F.col("raw_data")[47].cast(DoubleType())).alias("actor2_geo_long"),
        F.col("raw_data")[48].alias("actor2_geo_feature_id"),
        # 사건 지리정보
        F.col("raw_data")[49].alias("action_geo_type"),
        F.col("raw_data")[50].alias("action_geo_fullname"),
        F.col("raw_data")[51].alias("action_geo_country_code"),
        F.col("raw_data")[52].alias("action_geo_adm1_code"),
        F.when(F.col("raw_data")[53] != "", F.col("raw_data")[53].cast(DoubleType())).alias("action_geo_lat"),
        F.when(F.col("raw_data")[54] != "", F.col("raw_data")[54].cast(DoubleType())).alias("action_geo_long"),
        F.col("raw_data")[55].alias("action_geo_feature_id"),
        # 추가 정보
        F.col("raw_data")[56].alias("date_added"),
        F.col("raw_data")[57].alias("source_url"),
        F.lit(None).cast(StringType()).alias("actor1_geo_centroid"),
        F.lit(None).cast(StringType()).alias("actor2_geo_centroid"),
        F.lit(None).cast(StringType()).alias("action_geo_centroid"),
        # 메타데이터
        F.current_timestamp().alias("processed_time"),
        F.lit(f"kafka_backfill_{start_date}_{end_date}").alias("source_file"),
    )

    # 빈 문자열을 NULL로 변환
    for col_name in silver_df.columns:
        if silver_df.schema[col_name].dataType == StringType():
            silver_df = silver_df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
            )

    return silver_df


def write_to_silver(df: DataFrame, silver_path: str):
    """변환된 DataFrame을 Silver Layer에 append"""
    logging.info("Saving backfill data to Silver Delta Table...")
    record_count = df.count()
    if record_count == 0:
        logging.warning("No records to save!")
        return

    (
        df.write.format("delta")
        .mode("append")
        .save(silver_path)
    )
    logging.info(f"Successfully saved {record_count} records to {silver_path}")


def run_backfill(start_date, end_date):
    """지정된 기간 동안 Kafka에서 데이터를 가져와 Silver Layer로 백필 처리"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting GDELT 1-month backfill: {start_date} to {end_date}")

    # Spark 세션 생성 (Kafka 지원 포함)
    spark = get_spark_session(
        f"GDELT_Backfill_{start_date}_to_{end_date}",
        "spark://spark-master:7077"
    )

    # Redis에 드라이버 UI 정보 등록
    redis_client.register_driver_ui(spark, f"GDELT Backfill {start_date}-{end_date}")

    try:
        # 1. Kafka에서 타임스탬프 범위의 히스토리컬 데이터 읽기
        parsed_df = read_from_kafka(spark, start_date, end_date)

        if parsed_df.rdd.isEmpty():
            logger.warning(f"No data found in Kafka for period {start_date} to {end_date}")
            return

        # 2. 데이터 변환 (gdelt_silver_processor와 동일)
        silver_df = transform_raw_to_silver(parsed_df, start_date, end_date)

        # 3. Silver Layer에 저장
        write_to_silver(silver_df, "s3a://warehouse/silver/gdelt_events")

        # 4. 샘플 데이터 확인
        logger.info("Sample backfilled Silver data:")
        silver_df.select(
            "global_event_id",
            "day",
            "actor1_country_code",
            "event_root_code",
            "avg_tone"
        ).show(5)

        logger.info(f"Backfill completed successfully for {start_date} to {end_date}")

    except Exception as e:
        logger.error(f"Error during backfill process: {e}", exc_info=True)
        raise

    finally:
        # Redis에서 드라이버 UI 정보 정리
        redis_client.unregister_driver_ui(spark)
        spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    # Airflow에서 넘겨주는 인자(argument)를 받음
    if len(sys.argv) != 3:
        print("Usage: backfill_runner.py <start_date> <end_date>")
        sys.exit(1)
    
    start_date_arg = sys.argv[1]
    end_date_arg = sys.argv[2]
    
    run_backfill(start_date_arg, end_date_arg)