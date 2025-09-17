"""
GDELT 3-Way 조인 처리기
Events + Mentions + GKG 데이터 조인
"""

from pyspark.sql import DataFrame, functions as F
import logging

logger = logging.getLogger(__name__)


def perform_three_way_join(
    events_silver: DataFrame,
    mentions_silver: DataFrame = None,
    gkg_silver: DataFrame = None,
) -> DataFrame:
    """
    Events, Mentions, GKG 3-way 조인 수행

    Args:
        events_silver: Events Silver DataFrame (필수)
        mentions_silver: Mentions Silver DataFrame (선택)
        gkg_silver: GKG Silver DataFrame (선택)

    Returns:
        DataFrame: 조인된 결과
    """
    logger.info("Performing 3-Way Join for detailed analysis...")

    if events_silver is None:
        raise ValueError("Events data is required for 3-way join")

    # 1: Events와 Mentions를 Join
    if mentions_silver is not None:
        logger.info("...Joining Events with Mentions")
        events_mentions_joined = events_silver.join(
            mentions_silver,
            events_silver["global_event_id"] == mentions_silver["global_event_id"],
            "left",
        ).drop(
            mentions_silver["global_event_id"],
            mentions_silver["extras"],
            mentions_silver["source_file"],
        )
    else:
        logger.warning("No Mentions data found. Skipping join with Mentions.")
        events_mentions_joined = events_silver

    # 2: 위 결과와 GKG를 Join
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
            "No GKG data found or join key is missing. Skipping join with GKG."
        )
        final_joined_df = events_mentions_joined

    return final_joined_df


def select_final_columns(df: DataFrame) -> DataFrame:
    """최종 스키마에 맞춰 컬럼 선택"""
    logger.info("Selecting final columns for the unified Silver schema...")

    final_df = df.select(
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
            if "mention_identifier" in df.columns
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
            if "gkg_record_id" in df.columns
            else []
        ),
        # 메타데이터 (Events Detailed 스키마에 맞춰 alias)
        F.col("processed_at"),
        F.col("source_file"),
        # 우선순위 날짜 컬럼
        *([F.col("priority_date")] if "priority_date" in df.columns else []),
        # 파티션 컬럼들 (있는 경우만)
        *([F.col("year")] if "year" in df.columns else []),
        *([F.col("month")] if "month" in df.columns else []),
        *([F.col("day")] if "day" in df.columns else []),
        *([F.col("hour")] if "hour" in df.columns else []),
    ).distinct()

    return final_df
