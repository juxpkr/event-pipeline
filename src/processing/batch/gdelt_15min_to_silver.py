"""
GDELT Silver Processor - Kafka Raw 데이터를 읽어서 정제 후 Silver Delta Table로 저장
"""

import sys
from pathlib import Path

# sys.path.append("/app") 대신, 이 파일의 위치를 기준으로 프로젝트 루트를 찾아서 경로에 추가한다.
# 이렇게 하면 어떤 환경에서 실행해도 항상 프로젝트의 src 폴더를 찾을 수 있다.
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_gdelt_silver_schema():
    """GDELT Silver Table 스키마 정의 (GDELT 2.0 코드북 기준)"""
    return StructType(
        [
            # 기본 식별자
            StructField("global_event_id", LongType(), False),
            StructField("event_date", DateType(), True),
            # 주체(Actor1) 정보
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
            # 대상(Actor2) 정보
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
            # 이벤트 정보
            StructField("is_root_event", IntegerType(), True),
            StructField("event_code", StringType(), True),
            StructField("event_base_code", StringType(), True),
            StructField("event_root_code", StringType(), True),
            StructField("quad_class", IntegerType(), True),
            StructField("goldstein_scale", DoubleType(), True),
            StructField("num_mentions", IntegerType(), False),
            StructField("num_sources", IntegerType(), False),
            StructField("num_articles", IntegerType(), False),
            StructField("avg_tone", DoubleType(), True),
            # 주체1 지리정보
            StructField("actor1_geo_type", IntegerType(), True), # 코드북 기준: Integer
            StructField("actor1_geo_fullname", StringType(), True),
            StructField("actor1_geo_country_code", StringType(), True),
            StructField("actor1_geo_adm1_code", StringType(), True),
            StructField("actor1_geo_adm2_code", StringType(), True),
            StructField("actor1_geo_lat", DoubleType(), True),
            StructField("actor1_geo_long", DoubleType(), True),
            StructField("actor1_geo_feature_id", StringType(), True),
            # 대상2 지리정보
            StructField("actor2_geo_type", IntegerType(), True), # 코드북 기준: Integer
            StructField("actor2_geo_fullname", StringType(), True),
            StructField("actor2_geo_country_code", StringType(), True),
            StructField("actor2_geo_adm1_code", StringType(), True),
            StructField("actor2_geo_adm2_code", StringType(), True),
            StructField("actor2_geo_lat", DoubleType(), True),
            StructField("actor2_geo_long", DoubleType(), True),
            StructField("actor2_geo_feature_id", StringType(), True),
            # 사건 지리정보
            StructField("action_geo_type", IntegerType(), True), # 코드북 기준: Integer
            StructField("action_geo_fullname", StringType(), True),
            StructField("action_geo_country_code", StringType(), True),
            StructField("action_geo_adm1_code", StringType(), True),
            StructField("action_geo_adm2_code", StringType(), True),
            StructField("action_geo_lat", DoubleType(), True),
            StructField("action_geo_long", DoubleType(), True),
            StructField("action_geo_feature_id", StringType(), True),
            # 추가 정보
            StructField("date_added", TimestampType(), True),
            StructField("source_url", StringType(), True),
            # 메타데이터
            StructField("processed_time", TimestampType(), False),
            StructField("source_file", StringType(), True),
        ]
    )


def transform_raw_to_silver(raw_df: DataFrame) -> DataFrame:
    """Raw 데이터를 Silver 스키마에 맞게 정제하고 변환 (GDELT 2.0 코드북 기준)"""

    # 1. 데이터 유효성 검사: raw_data 배열의 크기가 최소 58개 이상인 데이터만 처리
    min_expected_columns = 58
    
    valid_df = raw_df.filter(F.size("raw_data") >= min_expected_columns)
    invalid_df = raw_df.filter(F.size("raw_data") < min_expected_columns)

    invalid_count = invalid_df.count()
    if invalid_count > 0:
        logger.warning(f"⚠️ Found {invalid_count} records with less than {min_expected_columns} columns. These records will be skipped.")

    # 2. raw_data 배열에서 각 컬럼 추출 및 타입 캐스팅
    silver_df = valid_df.select(
        # 기본 식별자 (0-4)
        F.col("raw_data")[0].cast(LongType()).alias("global_event_id"),
        F.col("raw_data")[1].alias("event_date_str"),
        # Actor1 (5-14)
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
        # Actor2 (15-24)
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
        # Event (25-34)
        F.col("raw_data")[25].cast(IntegerType()).alias("is_root_event"),
        F.col("raw_data")[26].alias("event_code"),
        F.col("raw_data")[27].alias("event_base_code"),
        F.col("raw_data")[28].alias("event_root_code"),
        F.col("raw_data")[29].cast(IntegerType()).alias("quad_class"),
        F.col("raw_data")[30].cast(DoubleType()).alias("goldstein_scale"),
        F.col("raw_data")[31].cast(IntegerType()).alias("num_mentions"),
        F.col("raw_data")[32].cast(IntegerType()).alias("num_sources"),
        F.col("raw_data")[33].cast(IntegerType()).alias("num_articles"),
        F.col("raw_data")[34].cast(DoubleType()).alias("avg_tone"),
        # Actor1 Geo (35-42)
        F.col("raw_data")[35].cast(IntegerType()).alias("actor1_geo_type"),
        F.col("raw_data")[36].alias("actor1_geo_fullname"),
        F.col("raw_data")[37].alias("actor1_geo_country_code"),
        F.col("raw_data")[38].alias("actor1_geo_adm1_code"),
        F.col("raw_data")[39].alias("actor1_geo_adm2_code"),
        F.col("raw_data")[40].cast(DoubleType()).alias("actor1_geo_lat"),
        F.col("raw_data")[41].cast(DoubleType()).alias("actor1_geo_long"),
        F.col("raw_data")[42].alias("actor1_geo_feature_id"),
        # Actor2 Geo (43-50)
        F.col("raw_data")[43].cast(IntegerType()).alias("actor2_geo_type"),
        F.col("raw_data")[44].alias("actor2_geo_fullname"),
        F.col("raw_data")[45].alias("actor2_geo_country_code"),
        F.col("raw_data")[46].alias("actor2_geo_adm1_code"),
        F.col("raw_data")[47].alias("actor2_geo_adm2_code"),
        F.col("raw_data")[48].cast(DoubleType()).alias("actor2_geo_lat"),
        F.col("raw_data")[49].cast(DoubleType()).alias("actor2_geo_long"),
        F.col("raw_data")[50].alias("actor2_geo_feature_id"),
        # Action Geo (51-58)
        F.col("raw_data")[51].cast(IntegerType()).alias("action_geo_type"),
        F.col("raw_data")[52].alias("action_geo_fullname"),
        F.col("raw_data")[53].alias("action_geo_country_code"),
        F.col("raw_data")[54].alias("action_geo_adm1_code"),
        F.col("raw_data")[55].alias("action_geo_adm2_code"),
        F.col("raw_data")[56].cast(DoubleType()).alias("action_geo_lat"),
        F.col("raw_data")[57].cast(DoubleType()).alias("action_geo_long"),
        F.col("raw_data")[58].alias("action_geo_feature_id"),
        # Data Mgmt (59-60)
        F.col("raw_data")[59].alias("date_added_str"),
        F.col("raw_data")[60].alias("source_url"),
        # 메타데이터
        F.current_timestamp().alias("processed_time"),
        F.col("source_file"),
    ).filter(F.col("global_event_id").isNotNull())

    # 3. 데이터 정제 및 변환
    silver_df = silver_df.withColumn(
        "event_date", F.to_date(F.col("event_date_str"), "yyyyMMdd")
    ).withColumn(
        "date_added", F.to_timestamp(F.col("date_added_str"), "yyyyMMddHHmmss")
    ).drop("event_date_str", "date_added_str")

    # 4. 빈 문자열을 NULL로 변환
    string_columns = [f.name for f in silver_df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_columns:
        silver_df = silver_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
        )

    # 5. NULL 값을 기본값(0)으로 채우기
    silver_df = silver_df.fillna({
        "num_mentions": 0,
        "num_sources": 0,
        "num_articles": 0
    })
    
    # 6. 최종 스키마에 맞게 컬럼 순서 정리 및 선택
    final_columns = [f.name for f in get_gdelt_silver_schema().fields]
    silver_df = silver_df.select(final_columns)

    return silver_df


def setup_silver_table(
    spark: SparkSession, table_name: str, silver_path: str, schema: StructType
):
    # 경쟁 상태를 방지하기 위해 Silver 테이블 구조를 미리 생성
    db_name = table_name.split(".")[0]
    logger.info(
        f"🚩 Preemptively creating database '{db_name}' and table '{table_name}'..."
    )
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
    # Kafka에서 Raw 데이터를 읽어 DataFrame으로 반환
    logger.info("📥 Reading RAW data from Kafka...")
    raw_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "gdelt_raw_events")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
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
    return parsed_df


def write_to_silver(df: DataFrame, silver_path: str):
    # 변환된 DataFrame을 Silver Layer에 덮어쓴다 (단일 파일로)
    logger.info("💾 Saving data to Silver Delta Table...")
    record_count = df.count()
    if record_count == 0:
        logger.warning("⚠️ No records to save!")
        return

    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    logger.info(f"🎉 Successfully saved {record_count} records to {silver_path}")


def main():
    # 메인 실행 함수
    logger.info("🚀 Starting GDELT Silver Processor...")

    # Kafka 지원을 위해 get_spark_session 사용
    spark = get_spark_session("GDELT Silver Processor", "spark://spark-master:7077")

    try:
        # 1. 빈 테이블을 선점 해야함.
        silver_schema = get_gdelt_silver_schema()
        setup_silver_table(
            spark,
            "default.gdelt_silver_events",
            "s3a://warehouse/silver/gdelt_events",
            silver_schema,
        )

        # 2. 데이터 처리 로직
        parsed_df = read_from_kafka(spark)
        if parsed_df.rdd.isEmpty():
            logger.warning("⚠️ No RAW data found in Kafka. Exiting gracefully.")
            return

        # 3. 데이터 변환
        silver_df = transform_raw_to_silver(parsed_df)

        # 4. 데이터 저장
        write_to_silver(silver_df, "s3a://warehouse/silver/gdelt_events")

        # 5. 샘플 데이터 확인
        logger.info("🔍 Sample final Silver data:")
        silver_df.select(
            "global_event_id",
            "event_date",
            "actor1_country_code",
            "event_root_code",
            "avg_tone",
            "num_mentions"
        ).show(5)

    except Exception as e:
        logger.error(f"❌ Error in Silver processing: {e}", exc_info=True)

    finally:
        spark.stop()
        logger.info("✅ Spark session closed")


if __name__ == "__main__":
    main()
