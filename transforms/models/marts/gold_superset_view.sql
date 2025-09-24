-- [Marts 테이블] : models/marts/gold_superset_view.sql
-- Version : 1.0
-- 역할: Superset 연결용 통합 테이블

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='view'
) }}

WITH realtime_summary AS (
    -- 1. 15분마다 업데이트되는 일일/국가별 KPI 요약 테이블
    SELECT * FROM {{ ref('gold_near_realtime_summary') }}
),

representative_stories AS (
    -- 2. 하루/국가별로 가장 중요한(Goldstein Scale이 가장 낮은) 이벤트의 스토리를 하나씩 선택
    SELECT
        event_date,
        mp_action_geo_country_iso,
        simple_story AS representative_simple_story,
        headline_story AS representative_headline_story,
        -- Window Function을 사용하여 각 그룹 내에서 순위를 매깁니다.
        ROW_NUMBER() OVER (PARTITION BY event_date, mp_action_geo_country_iso ORDER BY goldstein_scale ASC) as rn
    FROM {{ ref('gold_daily_detailed_events') }}
)

-- 3. 최종 SELECT: KPI 요약 정보와 대표 스토리 정보를 조인합니다.
SELECT
    -- 실시간 요약 정보
    summary.event_date,
    summary.mp_action_geo_country_iso AS action_country_iso,
    summary.mp_action_geo_country_eng AS action_country_eng,
    summary.mp_action_geo_country_kor AS action_country_kor,
    summary.risk_score,
    summary.event_count,
    summary.avg_goldstein_scale,
    summary.avg_tone,
    summary.total_articles,
    summary.count_cooperation_event,
    summary.count_conflict_event,
    summary.count_anomaly_event,
    
    -- 일일 상세 정보 (15시간 지연)
    stories.representative_simple_story,
    stories.representative_headline_story,
    
    summary.processed_at AS updated_at

FROM realtime_summary AS summary
LEFT JOIN representative_stories AS stories
    ON summary.event_date = stories.event_date 
    AND summary.mp_action_geo_country_iso = stories.mp_action_geo_country_iso