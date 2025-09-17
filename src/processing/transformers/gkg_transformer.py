"""
GDELT GKG 데이터 변환기
Bronze → Silver 변환 로직
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType
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


def transform_gkg_to_silver(df: DataFrame) -> DataFrame:
    """GKG Bronze 데이터를 Silver로 변환"""
    logger.info("Transforming GKG Bronze to Silver...")

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
        F.col("bronze_data")[27].alias("extras"),
        # 메타데이터
        F.current_timestamp().alias("gkg_processed_time"),
        F.col("source_file"),
    ).filter(F.col("gkg_record_id").isNotNull())

    # 빈 문자열 처리
    silver_df = clean_string_fields(silver_df)

    return silver_df
