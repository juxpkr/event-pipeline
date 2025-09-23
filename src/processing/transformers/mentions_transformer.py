"""
GDELT Mentions 데이터 변환기
Bronze → Silver 변환 로직
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, DoubleType
import logging

logger = logging.getLogger(__name__)


def clean_string_fields(df: DataFrame) -> DataFrame:
    """빈 문자열을 NULL로 변환하는 데이터 정제 함수"""
    from pyspark.sql.types import StringType

    string_columns = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    ]

    for col_name in string_columns:
        df = df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)),
        )

    return df


def transform_mentions_to_silver(df: DataFrame) -> DataFrame:
    """Mentions Bronze 데이터를 Silver로 변환"""
    logger.info("Transforming Mentions Bronze to Silver...")

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
        F.col("bronze_data")[15].alias("extras"),
        # 메타데이터
        F.current_timestamp().alias("mentions_processed_time"),
        F.col("source_file"),
    ).filter(F.col("global_event_id").isNotNull())

    # 빈 문자열 처리
    silver_df = clean_string_fields(silver_df)

    return silver_df
