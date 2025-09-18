"""
GDELT 데이터 스키마 정의
Events 2.0, Mentions, GKG 데이터의 컬럼 구조 및 타입 정의
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
    ArrayType,
)
from typing import Dict, List


class GDELTSchemas:
    """GDELT 데이터 타입별 스키마 정의 클래스"""

    @staticmethod
    def get_events_schema() -> StructType:
        """
        GDELT Events 2.0 스키마
        총 61개 컬럼
        """
        return StructType(
            [
                # 기본 식별자
                StructField("GLOBALEVENTID", StringType(), True),
                StructField("SQLDATE", StringType(), True),
                StructField("MonthYear", StringType(), True),
                StructField("Year", IntegerType(), True),
                StructField("FractionDate", DoubleType(), True),
                # Actor 1 정보
                StructField("Actor1Code", StringType(), True),
                StructField("Actor1Name", StringType(), True),
                StructField("Actor1CountryCode", StringType(), True),
                StructField("Actor1KnownGroupCode", StringType(), True),
                StructField("Actor1EthnicCode", StringType(), True),
                StructField("Actor1Religion1Code", StringType(), True),
                StructField("Actor1Religion2Code", StringType(), True),
                StructField("Actor1Type1Code", StringType(), True),
                StructField("Actor1Type2Code", StringType(), True),
                StructField("Actor1Type3Code", StringType(), True),
                # Actor 2 정보
                StructField("Actor2Code", StringType(), True),
                StructField("Actor2Name", StringType(), True),
                StructField("Actor2CountryCode", StringType(), True),
                StructField("Actor2KnownGroupCode", StringType(), True),
                StructField("Actor2EthnicCode", StringType(), True),
                StructField("Actor2Religion1Code", StringType(), True),
                StructField("Actor2Religion2Code", StringType(), True),
                StructField("Actor2Type1Code", StringType(), True),
                StructField("Actor2Type2Code", StringType(), True),
                StructField("Actor2Type3Code", StringType(), True),
                # 이벤트 액션 정보
                StructField("IsRootEvent", IntegerType(), True),
                StructField("EventCode", StringType(), True),
                StructField("EventBaseCode", StringType(), True),
                StructField("EventRootCode", StringType(), True),
                StructField("QuadClass", IntegerType(), True),
                StructField("GoldsteinScale", DoubleType(), True),
                StructField("NumMentions", IntegerType(), True),
                StructField("NumSources", IntegerType(), True),
                StructField("NumArticles", IntegerType(), True),
                StructField("AvgTone", DoubleType(), True),
                # Actor1 지리적 정보
                StructField("Actor1Geo_Type", IntegerType(), True),
                StructField("Actor1Geo_FullName", StringType(), True),
                StructField("Actor1Geo_CountryCode", StringType(), True),
                StructField("Actor1Geo_ADM1Code", StringType(), True),
                StructField("Actor1Geo_ADM2Code", StringType(), True),
                StructField("Actor1Geo_Lat", DoubleType(), True),
                StructField("Actor1Geo_Long", DoubleType(), True),
                StructField("Actor1Geo_FeatureID", StringType(), True),
                # Actor2 지리적 정보
                StructField("Actor2Geo_Type", IntegerType(), True),
                StructField("Actor2Geo_FullName", StringType(), True),
                StructField("Actor2Geo_CountryCode", StringType(), True),
                StructField("Actor2Geo_ADM1Code", StringType(), True),
                StructField("Actor2Geo_ADM2Code", StringType(), True),
                StructField("Actor2Geo_Lat", DoubleType(), True),
                StructField("Actor2Geo_Long", DoubleType(), True),
                StructField("Actor2Geo_FeatureID", StringType(), True),
                # 액션 지리적 정보
                StructField("ActionGeo_Type", IntegerType(), True),
                StructField("ActionGeo_FullName", StringType(), True),
                StructField("ActionGeo_CountryCode", StringType(), True),
                StructField("ActionGeo_ADM1Code", StringType(), True),
                StructField("ActionGeo_ADM2Code", StringType(), True),
                StructField("ActionGeo_Lat", DoubleType(), True),
                StructField("ActionGeo_Long", DoubleType(), True),
                StructField("ActionGeo_FeatureID", StringType(), True),
                # 날짜 정보
                StructField("DATEADDED", StringType(), True),
                StructField("SOURCEURL", StringType(), True),
            ]
        )

    @staticmethod
    def get_mentions_schema() -> StructType:
        """
        GDELT Mentions 스키마
        총 16개 컬럼
        """
        return StructType(
            [
                StructField("GLOBALEVENTID", StringType(), True),
                StructField("EventTimeDate", StringType(), True),
                StructField("MentionTimeDate", StringType(), True),
                StructField("MentionType", IntegerType(), True),
                StructField("MentionSourceName", StringType(), True),
                StructField(
                    "MentionIdentifier", StringType(), True
                ),  # DocumentIdentifier
                StructField("SentenceID", IntegerType(), True),
                StructField("Actor1CharOffset", IntegerType(), True),
                StructField("Actor2CharOffset", IntegerType(), True),
                StructField("ActionCharOffset", IntegerType(), True),
                StructField("InRawText", IntegerType(), True),
                StructField("Confidence", IntegerType(), True),
                StructField("MentionDocLen", IntegerType(), True),
                StructField("MentionDocTone", DoubleType(), True),
                StructField("MentionDocTranslationInfo", StringType(), True),
                StructField("Extras", StringType(), True),
            ]
        )

    @staticmethod
    def get_gkg_schema() -> StructType:
        """
        GDELT GKG (Global Knowledge Graph) 스키마
        총 27개 컬럼 - 매우 복잡한 구조
        """
        return StructType(
            [
                StructField("GKGRECORDID", StringType(), True),
                StructField("DATE", StringType(), True),
                StructField("SourceCollectionIdentifier", IntegerType(), True),
                StructField("SourceCommonName", StringType(), True),
                StructField("DocumentIdentifier", StringType(), True),  # 조인키
                StructField("Counts", StringType(), True),  # 세미콜론 구분 리스트
                StructField("V2Counts", StringType(), True),  # 세미콜론 구분 리스트
                StructField("Themes", StringType(), True),  # 세미콜론 구분 리스트
                StructField("V2Themes", StringType(), True),  # 세미콜론 구분 리스트
                StructField(
                    "V2EnhancedThemes", StringType(), True
                ),  # 세미콜론 구분 enhanced themes
                StructField("Locations", StringType(), True),  # 세미콜론 구분 복합 구조
                StructField(
                    "V2Locations", StringType(), True
                ),  # 세미콜론 구분 복합 구조
                StructField("Persons", StringType(), True),  # 세미콜론 구분 리스트
                StructField("V2Persons", StringType(), True),  # 세미콜론 구분 리스트
                StructField(
                    "Organizations", StringType(), True
                ),  # 세미콜론 구분 리스트
                StructField(
                    "V2Organizations", StringType(), True
                ),  # 세미콜론 구분 리스트
                StructField("V2Tone", StringType(), True),  # 쉼표 구분 6개 값
                StructField("Dates", StringType(), True),  # 세미콜론 구분 리스트
                StructField("GCAM", StringType(), True),  # 쉼표 구분 복합 구조
                StructField("SharingImage", StringType(), True),
                StructField(
                    "RelatedImages", StringType(), True
                ),  # 세미콜론 구분 리스트
                StructField(
                    "SocialImageEmbeds", StringType(), True
                ),  # 세미콜론 구분 리스트
                StructField(
                    "SocialVideoEmbeds", StringType(), True
                ),  # 세미콜론 구분 리스트
                StructField("Quotations", StringType(), True),  # 파이프 구분 복합 구조
                StructField("AllNames", StringType(), True),  # 세미콜론 구분 리스트
                StructField("Amounts", StringType(), True),  # 세미콜론 구분 리스트
                StructField("TranslationInfo", StringType(), True),
                StructField("Extras", StringType(), True),
            ]
        )

    @staticmethod
    def get_silver_events_schema() -> StructType:
        """
        Silver Layer Events 스키마 (GDELT 2.0 코드북 완전 준수)

        DE2님이 설계한 완전체 스키마를 기준으로 구현
        DA팀이 이미 이 스키마 기준으로 dbt 모델링 작업을 수행함
        """
        return StructType(
            [
                # 기본 식별자
                StructField("global_event_id", StringType(), False),
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
                StructField("actor1_geo_type", IntegerType(), True),
                StructField("actor1_geo_fullname", StringType(), True),
                StructField("actor1_geo_country_code", StringType(), True),
                StructField("actor1_geo_adm1_code", StringType(), True),
                StructField("actor1_geo_adm2_code", StringType(), True),
                StructField("actor1_geo_lat", DoubleType(), True),
                StructField("actor1_geo_long", DoubleType(), True),
                StructField("actor1_geo_feature_id", StringType(), True),
                # 대상2 지리정보
                StructField("actor2_geo_type", IntegerType(), True),
                StructField("actor2_geo_fullname", StringType(), True),
                StructField("actor2_geo_country_code", StringType(), True),
                StructField("actor2_geo_adm1_code", StringType(), True),
                StructField("actor2_geo_adm2_code", StringType(), True),
                StructField("actor2_geo_lat", DoubleType(), True),
                StructField("actor2_geo_long", DoubleType(), True),
                StructField("actor2_geo_feature_id", StringType(), True),
                # 사건 지리정보
                StructField("action_geo_type", IntegerType(), True),
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
                StructField("processed_at", TimestampType(), False),
                StructField("source_file", StringType(), True),
                # 파티션 컬럼들
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
                StructField("hour", IntegerType(), True),
            ]
        )

    @staticmethod
    def get_silver_events_detailed_schema() -> StructType:
        """
        Events detailed Silver 스키마 (3-way 조인 후 핵심 컬럼만 선별)

        Events + Mentions + GKG 조인 결과에서 분석에 필수적인 컬럼만 포함
        """
        return StructType(
            [
                # Events 핵심 필드
                StructField("global_event_id", StringType(), False),
                StructField("event_date", DateType(), True),
                StructField("actor1_code", StringType(), True),
                StructField("actor1_name", StringType(), True),
                StructField("actor1_country_code", StringType(), True),
                StructField("actor2_code", StringType(), True),
                StructField("actor2_name", StringType(), True),
                StructField("actor2_country_code", StringType(), True),
                StructField("event_code", StringType(), True),
                StructField("event_base_code", StringType(), True),
                StructField("goldstein_scale", DoubleType(), True),
                StructField("num_mentions", IntegerType(), True),
                StructField("num_articles", IntegerType(), True),
                StructField("avg_tone", DoubleType(), True),
                StructField("action_geo_fullname", StringType(), True),
                StructField("action_geo_country_code", StringType(), True),
                StructField("action_geo_lat", DoubleType(), True),
                StructField("action_geo_long", DoubleType(), True),
                StructField("source_url", StringType(), True),
                # Mentions 핵심 필드
                StructField("mention_source_name", StringType(), True),
                StructField("mention_doc_tone", DoubleType(), True),
                # GKG 핵심 필드 (분석에 가장 중요한 것들)
                StructField("v2_persons", StringType(), True),  # 인물
                StructField("v2_organizations", StringType(), True),  # 조직
                StructField("v2_enhanced_themes", StringType(), True),  # 테마
                StructField("amounts", StringType(), True),  # 금액/수량
                # 메타데이터
                StructField("processed_at", TimestampType(), False),
                # 파티션 컬럼들
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("day", IntegerType(), True),
                StructField("hour", IntegerType(), True),
            ]
        )

    @staticmethod
    def get_silver_mentions_schema() -> StructType:
        """Silver Layer Mentions 스키마 (정제된 버전)"""
        return StructType(
            [
                StructField("global_event_id", StringType(), False),
                StructField("mention_time_date", DateType(), True),
                StructField("mention_type", IntegerType(), True),
                StructField("mention_source_name", StringType(), True),
                StructField("document_identifier", StringType(), True),  # 조인키
                StructField("confidence", IntegerType(), True),
                StructField("mention_doc_tone", DoubleType(), True),
                StructField("mention_doc_len", IntegerType(), True),
                # 처리 메타데이터
                StructField("processed_at", TimestampType(), True),
                StructField("data_quality_score", DoubleType(), True),
            ]
        )

    @staticmethod
    def get_silver_gkg_schema() -> StructType:
        """Silver Layer GKG 스키마 (정제된 버전)"""
        return StructType(
            [
                StructField("gkg_record_id", StringType(), False),
                StructField("date", StringType(), True),
                StructField("source_common_name", StringType(), True),
                StructField("document_identifier", StringType(), True),  # 조인키
                # 파싱된 복합 필드들
                StructField("themes", ArrayType(StringType()), True),
                StructField("locations", ArrayType(StringType()), True),
                StructField("persons", ArrayType(StringType()), True),
                StructField("organizations", ArrayType(StringType()), True),
                # Tone 정보 (파싱됨)
                StructField("tone_positive", DoubleType(), True),
                StructField("tone_negative", DoubleType(), True),
                StructField("tone_polarity", DoubleType(), True),
                StructField("tone_activity", DoubleType(), True),
                StructField("tone_self_reference", DoubleType(), True),
                StructField("tone_word_count", IntegerType(), True),
                # 처리 메타데이터
                StructField("processed_at", TimestampType(), True),
                StructField("data_quality_score", DoubleType(), True),
            ]
        )


# 유틸리티 함수들
def get_column_mapping(data_type: str) -> Dict[int, str]:
    """
    Raw 데이터의 컬럼 인덱스를 필드명으로 매핑

    Args:
        data_type: 'events', 'mentions', 'gkg'

    Returns:
        Dict[int, str]: 컬럼 인덱스 -> 필드명 매핑
    """
    if data_type == "events":
        schema = GDELTSchemas.get_events_schema()
    elif data_type == "mentions":
        schema = GDELTSchemas.get_mentions_schema()
    elif data_type == "gkg":
        schema = GDELTSchemas.get_gkg_schema()
    else:
        raise ValueError(f"Unknown data type: {data_type}")

    return {i: field.name for i, field in enumerate(schema.fields)}
