"""
GDELT Silver Processor - Kafka Raw 데이터를 읽어서 정제 후 Silver Delta Table로 저장
"""

import sys

sys.path.append("/app")

from src.utils.spark_builder import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_gdelt_silver_schema():
    """GDELT Silver Table 스키마 정의 (전체 61개 컬럼)"""
    return StructType(
        [
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
        ]
    )


def transform_raw_to_silver(raw_df):
    """Raw 데이터를 Silver 스키마로 변환"""

    # raw_data 배열에서 각 컬럼 추출
    silver_df = raw_df.select(
        # 기본 식별자
        F.when(F.col("raw_data")[0] != "", F.col("raw_data")[0].cast(LongType())).alias(
            "global_event_id"
        ),
        F.when(
            F.col("raw_data")[1] != "", F.col("raw_data")[1].cast(IntegerType())
        ).alias("day"),
        F.when(
            F.col("raw_data")[2] != "", F.col("raw_data")[2].cast(IntegerType())
        ).alias("month_year"),
        F.when(
            F.col("raw_data")[3] != "", F.col("raw_data")[3].cast(IntegerType())
        ).alias("year"),
        F.when(
            F.col("raw_data")[4] != "", F.col("raw_data")[4].cast(DoubleType())
        ).alias("fraction_date"),
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
        F.when(
            F.col("raw_data")[25] != "", F.col("raw_data")[25].cast(IntegerType())
        ).alias("is_root_event"),
        F.col("raw_data")[26].alias("event_code"),
        F.col("raw_data")[27].alias("event_base_code"),
        F.col("raw_data")[28].alias("event_root_code"),
        F.when(
            F.col("raw_data")[29] != "", F.col("raw_data")[29].cast(IntegerType())
        ).alias("quad_class"),
        F.when(
            F.col("raw_data")[30] != "", F.col("raw_data")[30].cast(DoubleType())
        ).alias("goldstein_scale"),
        F.when(
            F.col("raw_data")[31] != "", F.col("raw_data")[31].cast(IntegerType())
        ).alias("num_mentions"),
        F.when(
            F.col("raw_data")[32] != "", F.col("raw_data")[32].cast(IntegerType())
        ).alias("num_sources"),
        F.when(
            F.col("raw_data")[33] != "", F.col("raw_data")[33].cast(IntegerType())
        ).alias("num_articles"),
        F.when(
            F.col("raw_data")[34] != "", F.col("raw_data")[34].cast(DoubleType())
        ).alias("avg_tone"),
        # 주체1 지리정보
        F.col("raw_data")[35].alias("actor1_geo_type"),
        F.col("raw_data")[36].alias("actor1_geo_fullname"),
        F.col("raw_data")[37].alias("actor1_geo_country_code"),
        F.col("raw_data")[38].alias("actor1_geo_adm1_code"),
        F.when(
            F.col("raw_data")[39] != "", F.col("raw_data")[39].cast(DoubleType())
        ).alias("actor1_geo_lat"),
        F.when(
            F.col("raw_data")[40] != "", F.col("raw_data")[40].cast(DoubleType())
        ).alias("actor1_geo_long"),
        F.col("raw_data")[41].alias("actor1_geo_feature_id"),
        # 대상2 지리정보
        F.col("raw_data")[42].alias("actor2_geo_type"),
        F.col("raw_data")[43].alias("actor2_geo_fullname"),
        F.col("raw_data")[44].alias("actor2_geo_country_code"),
        F.col("raw_data")[45].alias("actor2_geo_adm1_code"),
        F.when(
            F.col("raw_data")[46] != "", F.col("raw_data")[46].cast(DoubleType())
        ).alias("actor2_geo_lat"),
        F.when(
            F.col("raw_data")[47] != "", F.col("raw_data")[47].cast(DoubleType())
        ).alias("actor2_geo_long"),
        F.col("raw_data")[48].alias("actor2_geo_feature_id"),
        # 사건 지리정보
        F.col("raw_data")[49].alias("action_geo_type"),
        F.col("raw_data")[50].alias("action_geo_fullname"),
        F.col("raw_data")[51].alias("action_geo_country_code"),
        F.col("raw_data")[52].alias("action_geo_adm1_code"),
        F.when(
            F.col("raw_data")[53] != "", F.col("raw_data")[53].cast(DoubleType())
        ).alias("action_geo_lat"),
        F.when(
            F.col("raw_data")[54] != "", F.col("raw_data")[54].cast(DoubleType())
        ).alias("action_geo_long"),
        F.col("raw_data")[55].alias("action_geo_feature_id"),
        # 추가 정보
        F.col("raw_data")[56].alias("date_added"),
        F.col("raw_data")[57].alias("source_url"),
        # 나머지 컬럼들은 빈 값으로 처리 (GDELT 버전에 따라 다를 수 있음)
        F.lit(None).cast(StringType()).alias("actor1_geo_centroid"),
        F.lit(None).cast(StringType()).alias("actor2_geo_centroid"),
        F.lit(None).cast(StringType()).alias("action_geo_centroid"),
        # 메타데이터
        F.current_timestamp().alias("processed_time"),
        F.col("source_file"),
    )

    # 빈 문자열을 NULL로 변환
    for col_name in silver_df.columns:
        if silver_df.schema[col_name].dataType == StringType():
            silver_df = silver_df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
            )

    return silver_df


def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting GDELT Silver Processor...")

    # Spark 세션 생성
    spark = get_spark_session("GDELT_Silver_Processor", "spark://spark-master:7077")

    try:
        # Kafka에서 Raw 데이터 읽기
        logger.info("📥 Reading RAW data from Kafka...")
        raw_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "gdelt_raw_events")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if raw_df.count() == 0:
            logger.warning("⚠️ No RAW data found in Kafka. Run the Raw Producer first!")
            return

        # Kafka 메시지 파싱
        parsed_df = raw_df.select(
            F.from_json(
                F.col("value").cast("string"),
                StructType(
                    [
                        StructField("raw_data", ArrayType(StringType()), True),
                        StructField("row_number", IntegerType(), True),
                        StructField("source_file", StringType(), True),
                        StructField("extracted_time", StringType(), True),
                        StructField("source_url", StringType(), True),
                        StructField("total_columns", IntegerType(), True),
                    ]
                ),
            ).alias("data")
        ).select("data.*")

        logger.info("✅ Raw data parsed successfully")

        # Raw → Silver 변환
        logger.info("🔄 Transforming RAW data to Silver schema...")
        silver_df = transform_raw_to_silver(parsed_df)

        # 데이터 검증
        total_records = silver_df.count()
        logger.info(f"📊 Silver records: {total_records}")

        if total_records == 0:
            logger.warning("⚠️ No records to save!")
            return

        # Silver Delta Table로 저장 (정제된 데이터를 Silver 버킷에 저장)
        logger.info("💾 Saving to Silver Delta Table...")
        silver_path = "s3a://silver/gdelt_events"
        table_name = "default.gdelt_silver_events"

        logger.info("✍️ 데이터 저장 및 테이블 등록 중...")
        # 1단계: Delta Lake로 데이터 저장
        (silver_df.write.format("delta").mode("overwrite").save(silver_path))

        # 2단계: 메타스토어에 External Table 등록
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{silver_path}'
        """
        )

        logger.info(f"✅ 테이블 등록 성공: {table_name}")
        logger.info(f"📍 Delta Location: {silver_path}")
        logger.info(f"🎉 Successfully saved {total_records} records to Silver table!")
        logger.info(f"📍 Location: {silver_path}")

        # 샘플 데이터 확인
        logger.info("🔍 Sample Silver data:")
        silver_df.select(
            "global_event_id",
            "day",
            "actor1_country_code",
            "event_root_code",
            "avg_tone",
        ).show(5)

    except Exception as e:
        logger.error(f"❌ Error in Silver processing: {e}", exc_info=True)

    finally:
        spark.stop()
        logger.info("✅ Spark session closed")


if __name__ == "__main__":
    main()
