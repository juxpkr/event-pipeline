"""
GDELT Events 데이터 변환기
Bronze → Silver 변환 로직
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
import logging

logger = logging.getLogger(__name__)


def clean_string_fields(df: DataFrame) -> DataFrame:
    """빈 문자열을 NULL로 변환하는 데이터 정제 함수"""
    logger.info("Applying string field cleaning logic...")

    string_columns = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    ]

    for col_name in string_columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
        )

    logger.info(f"Cleaned {len(string_columns)} string fields")
    return df


def transform_events_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Events Bronze 데이터를 Silver 스키마에 맞게 정제하고 변환"""
    logger.info("Transforming Events data from Bronze to Silver...")

    min_expected_columns = 58
    valid_df = bronze_df.filter(F.size("bronze_data") >= min_expected_columns)

    silver_df = valid_df.select(
        # 기본 식별자 (0-4)
        F.col("bronze_data")[0].alias("global_event_id"),
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
        F.col("processed_at"),  # Bronze에서 가져온 수집시간 사용
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

    # 빈 문자열 처리
    silver_df = clean_string_fields(silver_df)

    return silver_df
