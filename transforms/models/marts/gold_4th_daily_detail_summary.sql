-- models/marts/gold_4th_daily_detail_summary.sql
-- [골드 테이블] 이벤트 상세 - 일일 요약

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

WITH events_mapped AS (
    -- 증분 처리를 위해 매핑된 Silver 데이터 참조
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

actors_descriptions AS (
    -- 최종 Actor 설명이 담긴 모델
    SELECT * FROM {{ ref('stg_actors_description') }}
),

-- 이상 징후(Anomaly) 탐지를 위해 Z-Score를 계산합니다.
z_score_calculation AS (
    SELECT
        global_event_id,
        -- Z-Score = (개별 값 - 전체 평균) / 전체 표준편차
        (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) AS goldstein_zscore,
        (avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0) AS tone_zscore
    FROM
        events_mapped
)

SELECT
    -- 필터(슬라이서)
    evt.event_date,
    evt.mp_event_categories AS event_category,
    evt.mp_action_location_country AS action_country,
    
    -- stg_actors_description에서 최종 Actor 정보를 가져옵니다.
    COALESCE(evt.actor1_name, actors.actor1_info) AS actor1_info,
    COALESCE(evt.actor2_name, actors.actor2_info) AS actor2_info,

    -- KPI 계산을 위한 컬럼
    evt.num_articles,
    
    -- 이상 징후 여부 플래그
    CASE
        WHEN z.goldstein_zscore > 2 OR z.tone_zscore < -2 OR z.tone_zscore > 2 THEN 1
        ELSE 0
    END AS is_anomaly,
    
    -- 협력/갈등 비율 계산을 위한 컬럼
    CASE 
        WHEN evt.mp_quad_class IN ('Verbal Cooperation', 'Material Cooperation') THEN 'Cooperation'
        WHEN evt.mp_quad_class IN ('Verbal Conflict', 'Material Conflict') THEN 'Conflict'
        ELSE 'Neutral'
    END AS event_type,
    
    evt.processed_at,

    -- 그래프를 위한 기본 지표
    evt.mp_event_info,
    evt.avg_tone,
    evt.num_mentions

FROM
    events_mapped AS evt
LEFT JOIN
    z_score_calculation AS z ON evt.global_event_id = z.global_event_id
LEFT JOIN
    actors_descriptions AS actors ON evt.global_event_id = actors.global_event_id

WHERE
    evt.event_date IS NOT NULL

{% if is_incremental() %}
  -- 이 모델이 이미 데이터를 가지고 있다면,
  -- 최신 날짜보다 더 새로운 데이터만 처리 (3일 버퍼 포함)
    AND event_date >= (SELECT date_add(MAX(event_date), -3) FROM {{ this }})

{% endif %}