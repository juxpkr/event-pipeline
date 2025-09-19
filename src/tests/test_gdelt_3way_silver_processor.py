"""
GDELT 3-Way Silver Processor - Kafka Bronze 데이터를 읽어서 정제 후 Silver 테이블로 저장

Medallion Architecture 적용:
- Bronze: Kafka에서 온 원본 데이터 (변환 없음)
- Silver: 정제되고 스키마가 적용된 분석 준비 데이터
"""

import os
import sys
from pathlib import Path

# Airflow 환경에서는 /opt/airflow가 프로젝트 루트
sys.path.append("/opt/airflow")

from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from test_gdelt_schemas import GDELTSchemas
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_string_fields(df: DataFrame) -> DataFrame:
    """
    빈 문자열을 NULL로 변환하는 데이터 정제 함수

    [팀원명]이 개발한 문자열 정제 로직을 기반으로 구현
    빈 문자열("")과 공백만 있는 문자열을 NULL로 변환하여 데이터 품질 향상

    Args:
        df: 정제할 DataFrame

    Returns:
        DataFrame: 문자열 필드가 정제된 DataFrame
    """
    logger.info("🧹 Applying string field cleaning logic...")

    # StringType 컬럼들만 추출
    string_columns = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    ]

    # 각 문자열 컬럼에 대해 빈 문자열 → NULL 변환
    for col_name in string_columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
        )

    logger.info(f"✅ Cleaned {len(string_columns)} string fields")
    return df


# 🧹 중복 스키마 함수들 제거됨 - test_gdelt_schemas.py의 GDELTSchemas 클래스 사용
# [팀원명]의 완전체 스키마를 표준으로 채택하여 중앙집중식 스키마 관리 구현


def transform_events_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Events Bronze 데이터를 Silver 스키마에 맞게 정제하고 변환"""
    logger.info("🔄 Transforming Events data from Bronze to Silver...")

    min_expected_columns = 58
    valid_df = bronze_df.filter(F.size("bronze_data") >= min_expected_columns)

    silver_df = valid_df.select(
        # 기본 식별자 (0-4)
        F.col("bronze_data")[0].cast(LongType()).alias("global_event_id"),
        F.col("bronze_data")[1].alias("event_date_str"),
        # Actor1 (5-14)
        F.col("bronze_data")[5].alias("actor1_code"),
        F.col("bronze_data")[6].alias("actor1_name"),
        F.col("bronze_data")[7].alias("actor1_country_code"),
        F.col("bronze_data")[8].alias("actor1_known_group_code"),
        F.col("bronze_data")[9].alias("actor1_ethnic_code"),
        F.col("bronze_data")[10].alias("actor1_religion1_code"),
        F.col("bronze_data")[11].alias("actor1_religion2_code"),
        F.col("bronze_data")[12].alias("actor1_type1_code"),
        F.col("bronze_data")[13].alias("actor1_type2_code"),
        F.col("bronze_data")[14].alias("actor1_type3_code"),
        # Actor2 (15-24)
        F.col("bronze_data")[15].alias("actor2_code"),
        F.col("bronze_data")[16].alias("actor2_name"),
        F.col("bronze_data")[17].alias("actor2_country_code"),
        F.col("bronze_data")[18].alias("actor2_known_group_code"),
        F.col("bronze_data")[19].alias("actor2_ethnic_code"),
        F.col("bronze_data")[20].alias("actor2_religion1_code"),
        F.col("bronze_data")[21].alias("actor2_religion2_code"),
        F.col("bronze_data")[22].alias("actor2_type1_code"),
        F.col("bronze_data")[23].alias("actor2_type2_code"),
        F.col("bronze_data")[24].alias("actor2_type3_code"),
        # Event (25-34)
        F.col("bronze_data")[25].cast(IntegerType()).alias("is_root_event"),
        F.col("bronze_data")[26].alias("event_code"),
        F.col("bronze_data")[27].alias("event_base_code"),
        F.col("bronze_data")[28].alias("event_root_code"),
        F.col("bronze_data")[29].cast(IntegerType()).alias("quad_class"),
        F.col("bronze_data")[30].cast(DoubleType()).alias("goldstein_scale"),
        F.col("bronze_data")[31].cast(IntegerType()).alias("num_mentions"),
        F.col("bronze_data")[32].cast(IntegerType()).alias("num_sources"),
        F.col("bronze_data")[33].cast(IntegerType()).alias("num_articles"),
        F.col("bronze_data")[34].cast(DoubleType()).alias("avg_tone"),
        # Action Geo (51-58)
        F.col("bronze_data")[51].cast(IntegerType()).alias("action_geo_type"),
        F.col("bronze_data")[52].alias("action_geo_fullname"),
        F.col("bronze_data")[53].alias("action_geo_country_code"),
        F.col("bronze_data")[54].alias("action_geo_adm1_code"),
        F.col("bronze_data")[55].alias("action_geo_adm2_code"),
        F.col("bronze_data")[56].cast(DoubleType()).alias("action_geo_lat"),
        F.col("bronze_data")[57].cast(DoubleType()).alias("action_geo_long"),
        F.col("bronze_data")[58].alias("action_geo_feature_id"),
        # Data Mgmt (59-60)
        F.col("bronze_data")[59].alias("date_added_str"),
        F.col("bronze_data")[60].alias("source_url"),
        # 메타데이터
        F.current_timestamp().alias("events_processed_time"),
        F.col("source_file"),
    ).filter(F.col("global_event_id").isNotNull())

    # 날짜 변환
    silver_df = (
        silver_df.withColumn(
            "event_date", F.to_date(F.col("event_date_str"), "yyyyMMdd")
        )
        .withColumn(
            "date_added", F.to_timestamp(F.col("date_added_str"), "yyyyMMddHHmmss")
        )
        .drop("event_date_str", "date_added_str")
    )

    # NULL 값 처리
    silver_df = silver_df.fillna(
        {"num_mentions": 0, "num_sources": 0, "num_articles": 0}
    )

    # 빈 문자열 처리 - [팀원]의 데이터 정제 로직 적용
    silver_df = clean_string_fields(silver_df)

    return silver_df


def setup_silver_table(
    spark: SparkSession, table_name: str, silver_path: str, schema: StructType
):
    """Silver 테이블 구조를 미리 생성"""
    db_name = table_name.split(".")[0]
    logger.info(f"🚩 Creating database '{db_name}' and table '{table_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .mode("ignore")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name, path=silver_path)
    )
    logger.info(f"🚩 Table '{table_name}' structure is ready at {silver_path}.")


def read_from_kafka(spark: SparkSession) -> DataFrame:
    """Kafka에서 Bronze 데이터를 읽어 DataFrame으로 반환"""
    logger.info("📥 Reading Bronze data from Kafka...")
    bronze_df = (
        spark.read.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        )
        .option(
            "subscribe", "gdelt_events_bronze,gdelt_mentions_bronze,gdelt_gkg_bronze"
        )
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Kafka 메시지 파싱
    parsed_df = bronze_df.select(
        F.from_json(
            F.col("value").cast("string"),
            StructType(
                [
                    StructField("data_type", StringType(), True),
                    StructField("bronze_data", ArrayType(StringType()), True),
                    StructField("row_number", IntegerType(), True),
                    StructField("source_file", StringType(), True),
                    StructField("extracted_time", StringType(), True),
                    StructField("source_url", StringType(), True),
                    StructField("total_columns", IntegerType(), True),
                    StructField(
                        "join_keys",
                        StructType(
                            [
                                StructField("GLOBALEVENTID", StringType(), True),
                                StructField("DATE", StringType(), True),
                                StructField("DocumentIdentifier", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
        ).alias("data"),
        F.col("topic"),
    ).select("data.*", "topic")
    return parsed_df


def transform_mentions_to_silver(df: DataFrame) -> DataFrame:
    """
    Mentions Bronze 데이터를 Silver로 변환
    """
    logger.info("🔄 Transforming Mentions Bronze to Silver...")

    silver_df = df.select(
        # 조인키
        F.col("bronze_data")[0].alias("global_event_id"),
        # 시간 정보
        F.col("bronze_data")[1].alias("event_time_date"),
        F.col("bronze_data")[2].alias("mention_time_date"),
        # Mention 기본 정보
        F.col("bronze_data")[3].cast(IntegerType()).alias("mention_type"),
        F.col("bronze_data")[4].alias("mention_source_name"),
        F.col("bronze_data")[5].alias("mention_identifier"),
        F.col("bronze_data")[6].cast(IntegerType()).alias("sentence_id"),
        # Character Offsets
        F.col("bronze_data")[7].cast(IntegerType()).alias("actor1_char_offset"),
        F.col("bronze_data")[8].cast(IntegerType()).alias("actor2_char_offset"),
        F.col("bronze_data")[9].cast(IntegerType()).alias("action_char_offset"),
        # 기타
        F.col("bronze_data")[10].cast(IntegerType()).alias("in_raw_text"),
        F.col("bronze_data")[11].cast(IntegerType()).alias("confidence"),
        F.col("bronze_data")[12].cast(IntegerType()).alias("mention_doc_len"),
        F.col("bronze_data")[13].cast(DoubleType()).alias("mention_doc_tone"),
        F.col("bronze_data")[14].alias("mention_doc_translation_info"),
        F.col("bronze_data")[15].alias("extras"),  # Extras 필드 추가
        # 메타데이터
        F.current_timestamp().alias("mentions_processed_time"),
        F.col("source_file"),
    ).filter(F.col("global_event_id").isNotNull())

    # 빈 문자열 처리
    silver_df = clean_string_fields(silver_df)

    return silver_df


def transform_gkg_to_silver(df: DataFrame) -> DataFrame:
    """
    GKG Bronze 데이터를 Silver로 변환
    """
    logger.info("🔄 Transforming GKG Bronze to Silver...")

    silver_df = df.select(
        # 기본 식별자
        F.col("bronze_data")[0].alias("gkg_record_id"),
        F.col("bronze_data")[1].alias("date"),
        F.col("bronze_data")[2]
        .cast(IntegerType())
        .alias("source_collection_identifier"),
        F.col("bronze_data")[3].alias("source_common_name"),
        F.col("bronze_data")[4].alias("document_identifier"),  # 조인키
        # 주요 컨텐츠 (복잡한 구조는 일단 String으로)
        F.col("bronze_data")[5].alias("counts"),
        F.col("bronze_data")[6].alias("v2_counts"),
        F.col("bronze_data")[7].alias("themes"),
        F.col("bronze_data")[8].alias("v2_themes"),
        F.col("bronze_data")[9].alias("v2_enhanced_themes"),
        F.col("bronze_data")[10].alias("locations"),
        F.col("bronze_data")[11].alias("v2_locations"),
        F.col("bronze_data")[12].alias("persons"),
        F.col("bronze_data")[13].alias("v2_persons"),
        F.col("bronze_data")[14].alias("organizations"),
        F.col("bronze_data")[15].alias("v2_organizations"),
        F.col("bronze_data")[16].alias("v2_tone"),
        F.col("bronze_data")[17].alias("dates"),
        F.col("bronze_data")[18].alias("gcam"),
        F.col("bronze_data")[19].alias("sharing_image"),
        F.col("bronze_data")[20].alias("related_images"),
        F.col("bronze_data")[21].alias("social_image_embeds"),
        F.col("bronze_data")[22].alias("social_video_embeds"),
        F.col("bronze_data")[23].alias("quotations"),
        F.col("bronze_data")[24].alias("all_names"),
        F.col("bronze_data")[25].alias("amounts"),
        F.col("bronze_data")[26].alias("translation_info"),
        F.col("bronze_data")[27].alias(
            "extras"
        ),  # Extras는 실제로 마지막 컬럼 (인덱스 27)
        # 메타데이터
        F.current_timestamp().alias("gkg_processed_time"),
        F.col("source_file"),
    ).filter(F.col("gkg_record_id").isNotNull())

    # 빈 문자열 처리
    silver_df = clean_string_fields(silver_df)

    return silver_df


def write_to_silver(df: DataFrame, silver_path: str, table_name: str):
    """변환된 DataFrame을 Silver Layer에 저장"""
    logger.info(f"💾 Saving {table_name} data to Silver Delta Table...")
    record_count = df.count()
    if record_count == 0:
        logger.warning(f"⚠️ No {table_name} records to save!")
        return

    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    logger.info(
        f"🎉 Successfully saved {record_count} {table_name} records to {silver_path}"
    )


def main():
    """메인 실행 함수"""
    logger.info("🚀 Starting GDELT 3-Way Silver Processor...")

    # Kafka 지원을 위해 get_spark_session 사용
    spark = get_spark_session(
        "GDELT 3Way Silver Processor", "spark://spark-master:7077"
    )

    # Redis에 드라이버 UI 정보 등록
    redis_client.register_driver_ui(spark, "GDELT 3Way Silver Processor")

    try:
        # 1. Silver 테이블들 설정 - Medallion Architecture 적용
        logger.info("🚩 Setting up Silver tables...")

        # 1-1. Events Silver (실시간 15분 마이크로배치용)
        setup_silver_table(
            spark,
            "default.gdelt_events",
            "s3a://warehouse/silver/gdelt_events",
            GDELTSchemas.get_silver_events_schema(),
        )

        # 1-2. Events detailed Silver (3-way 조인 통합 데이터)
        setup_silver_table(
            spark,
            "default.gdelt_events_detailed",
            "s3a://warehouse/silver/gdelt_events_detailed",
            GDELTSchemas.get_silver_events_detailed_schema(),
        )

        # 2. Kafka에서 데이터 읽기
        parsed_df = read_from_kafka(spark)
        if parsed_df.rdd.isEmpty():
            logger.warning("⚠️ No Bronze data found in Kafka. Exiting gracefully.")
            return

        # 3. 데이터 타입별로 분리 및 처리
        events_df = parsed_df.filter(F.col("data_type") == "events")
        mentions_df = parsed_df.filter(F.col("data_type") == "mentions")
        gkg_df = parsed_df.filter(F.col("data_type") == "gkg")

        # 4. Events 단독 Silver 처리 (실시간 15분 마이크로배치)
        events_silver = None
        if not events_df.rdd.isEmpty():
            events_silver = transform_events_to_silver(events_df)
            logger.info("💾 Saving Events to Silver...")

            # Events Silver 저장
            write_to_silver(
                events_silver, "s3a://warehouse/silver/gdelt_events", "Events"
            )

            logger.info("🔍 Events Silver sample:")
            events_silver.select(
                "global_event_id", "event_date", "actor1_country_code", "event_code"
            ).show(3)

        # 5. 3-Way 조인용 추가 변환
        mentions_silver = None
        gkg_silver = None

        if not mentions_df.rdd.isEmpty():
            mentions_silver = transform_mentions_to_silver(mentions_df)
            logger.info("🔍 Transformed Mentions data:")
            mentions_silver.select(
                "global_event_id", "mention_source_name", "confidence"
            ).show(3)

        if not gkg_df.rdd.isEmpty():
            gkg_silver = transform_gkg_to_silver(gkg_df)
            logger.info("🔍 Transformed GKG data:")
            gkg_silver.select(
                "gkg_record_id", "source_common_name", "document_identifier"
            ).show(3)

        # 6. 3-Way Join 및 Events detailed Silver 생성
        logger.info("🤝 Performing 3-Way Join for detailed analysis...")

        if events_silver is None:
            logger.error("❌ No Events data found. Cannot create unified table.")
            return

        # --- STEP 1: Events와 Mentions를 Join ---
        # Mentions 데이터가 없는 경우를 대비해 left join 사용.
        if mentions_silver is not None:
            logger.info("...Joining Events with Mentions")
            # Join의 명확성을 위해, 양쪽 DataFrame의 key 컬럼을 명시적으로 지정
            events_mentions_joined = events_silver.join(
                mentions_silver,
                events_silver["global_event_id"] == mentions_silver["global_event_id"],
                "left",
            ).drop(
                mentions_silver["global_event_id"],
                mentions_silver["extras"],
                mentions_silver["source_file"],
            )  # Join 후 중복된 컬럼들 제거
        else:
            logger.warning("⚠️ No Mentions data found. Skipping join with Mentions.")
            # Mentions 데이터가 없으면, Events 데이터만으로 다음 단계를 진행
            events_mentions_joined = events_silver

        # --- STEP 2: 위 결과와 GKG를 Join ---
        # GKG 데이터가 없거나, Join의 키가 되는 mention_identifier 컬럼이 없는 경우를 대비
        if (
            gkg_silver is not None
            and "mention_identifier" in events_mentions_joined.columns
        ):
            logger.info("...Joining result with GKG")
            final_joined_df = events_mentions_joined.join(
                gkg_silver,
                events_mentions_joined["mention_identifier"]
                == gkg_silver["document_identifier"],
                "left",
            ).drop(
                gkg_silver["extras"],
                gkg_silver["source_file"],
                gkg_silver["gkg_processed_time"],
            )
        else:
            logger.warning(
                "⚠️ No GKG data found or join key is missing. Skipping join with GKG."
            )
            final_joined_df = events_mentions_joined

        # --- STEP 3: 최종 스키마에 맞춰 컬럼 선택 및 이름 변경 (커팅 작업) ---
        logger.info(
            "🔪 Selecting and renaming final columns for the unified Silver schema..."
        )

        # 전체 컬럼을 포함한 Silver_detailed 테이블 (dbt에서 활용)
        final_silver_df = final_joined_df.select(
            # Events 컬럼들
            F.col("global_event_id"),
            F.col("event_date"),
            F.col("actor1_code"),
            F.col("actor1_name"),
            F.col("actor1_country_code"),
            F.col("actor1_known_group_code"),
            F.col("actor1_ethnic_code"),
            F.col("actor1_religion1_code"),
            F.col("actor1_religion2_code"),
            F.col("actor1_type1_code"),
            F.col("actor1_type2_code"),
            F.col("actor1_type3_code"),
            F.col("actor2_code"),
            F.col("actor2_name"),
            F.col("actor2_country_code"),
            F.col("actor2_known_group_code"),
            F.col("actor2_ethnic_code"),
            F.col("actor2_religion1_code"),
            F.col("actor2_religion2_code"),
            F.col("actor2_type1_code"),
            F.col("actor2_type2_code"),
            F.col("actor2_type3_code"),
            F.col("is_root_event"),
            F.col("event_code"),
            F.col("event_base_code"),
            F.col("event_root_code"),
            F.col("quad_class"),
            F.col("goldstein_scale"),
            F.col("num_mentions"),
            F.col("num_sources"),
            F.col("num_articles"),
            F.col("avg_tone"),
            F.col("action_geo_type"),
            F.col("action_geo_fullname"),
            F.col("action_geo_country_code"),
            F.col("action_geo_adm1_code"),
            F.col("action_geo_adm2_code"),
            F.col("action_geo_lat"),
            F.col("action_geo_long"),
            F.col("action_geo_feature_id"),
            F.col("date_added"),
            F.col("source_url"),
            # Mentions 컬럼들 (있는 경우만)
            *(
                [
                    F.col("event_time_date"),
                    F.col("mention_time_date"),
                    F.col("mention_type"),
                    F.col("mention_source_name"),
                    F.col("mention_identifier"),
                    F.col("sentence_id"),
                    F.col("actor1_char_offset"),
                    F.col("actor2_char_offset"),
                    F.col("action_char_offset"),
                    F.col("in_raw_text"),
                    F.col("confidence"),
                    F.col("mention_doc_len"),
                    F.col("mention_doc_tone"),
                    F.col("mention_doc_translation_info"),
                ]
                if "mention_identifier" in final_joined_df.columns
                else []
            ),
            # GKG 컬럼들 (있는 경우만)
            *(
                [
                    F.col("gkg_record_id"),
                    F.col("date"),
                    F.col("source_collection_identifier"),
                    F.col("source_common_name"),
                    F.col("document_identifier"),
                    F.col("counts"),
                    F.col("v2_counts"),
                    F.col("themes"),
                    F.col("v2_enhanced_themes"),
                    F.col("locations"),
                    F.col("v2_locations"),
                    F.col("persons"),
                    F.col("v2_persons"),
                    F.col("organizations"),
                    F.col("v2_organizations"),
                    F.col("v2_tone"),
                    F.col("dates"),
                    F.col("gcam"),
                    F.col("sharing_image"),
                    F.col("related_images"),
                    F.col("social_image_embeds"),
                    F.col("social_video_embeds"),
                    F.col("quotations"),
                    F.col("all_names"),
                    F.col("amounts"),
                    F.col("translation_info"),
                ]
                if "gkg_record_id" in final_joined_df.columns
                else []
            ),
            # 메타데이터 (각 테이블별 처리 시간 모두 보존)
            F.col("events_processed_time"),
            *(
                [F.col("mentions_processed_time")]
                if "mentions_processed_time" in final_joined_df.columns
                else []
            ),
            *(
                [F.col("gkg_processed_time")]
                if "gkg_processed_time" in final_joined_df.columns
                else []
            ),
            F.col("source_file"),
        ).distinct()

        # --- 7. Events detailed Silver 저장 ---
        write_to_silver(
            final_silver_df,
            "s3a://warehouse/silver/gdelt_events_detailed",
            "Events detailed GDELT",
        )

        logger.info("🔍 Sample of Events detailed Silver data:")
        final_silver_df.show(5, vertical=True)

        logger.info("🎉 Silver Layer processing completed successfully!")
        logger.info("📊 Two Silver tables created:")
        logger.info("   - gdelt_events: 실시간 15분 마이크로배치용")
        logger.info("   - gdelt_events_detailed: 3-way 조인 심화분석용")

    except Exception as e:
        logger.error(f"❌ Error in 3-Way Silver processing: {e}", exc_info=True)

    finally:
        try:
            logging.info(
                "✅ Job finished. Press Enter in the container's terminal to stop Spark session..."
            )
            input()
        except Exception:
            logging.info(
                "Running in non-interactive mode. Shutting down after job completion."
            )

        redis_client.unregister_driver_ui(spark)
        spark.stop()
        logger.info("✅ Spark session closed")


if __name__ == "__main__":
    main()
