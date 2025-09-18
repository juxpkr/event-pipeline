"""
날짜 우선순위 처리 모듈
Mentions > GKG > Events 순으로 가장 정확한 날짜 선택
"""

from pyspark.sql import DataFrame, functions as F
import logging

logger = logging.getLogger(__name__)


def create_priority_date(df: DataFrame) -> DataFrame:
    """
    우선순위 날짜 컬럼 생성
    1순위: Mentions mention_time_date (가장 정확)
    2순위: GKG date (기사 발행일)
    3순위: Events event_date (최후 보루)
    """
    logger.info("Creating priority date column...")

    df_with_priority = df.withColumn(
        "priority_date",
        F.coalesce(
            # 1순위: Mentions mention_time_date (가장 정확)
            F.to_timestamp(F.col("mention_time_date"), "yyyyMMddHHmmss"),
            # 2순위: GKG date (기사 발행일)
            F.to_timestamp(F.col("date"), "yyyyMMddHHmmss"),
            # 3순위: Events event_date (최후 보루)
            F.to_timestamp(F.col("event_date")),
            # 4순위: processed_at (처리 시간)
            F.col("processed_at"),
            # 5순위: 고정 fallback 날짜 (절대 null 방지)
            F.lit("1900-01-01T00:00:00Z").cast("timestamp"),
        ),
    )

    logger.info("Priority date column created")
    return df_with_priority
