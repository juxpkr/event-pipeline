-- models/marts/gold_3rd_events_summary.sql
-- [골드 테이블] 사건 레벨

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

WITH events AS (
    -- 증분 처리를 위해 매핑된 Silver 데이터 참조
    SELECT * FROM {{ ref('stg_seed_mapping') }}
)

SELECT
    global_event_id,
    event_date,
    actor1_name,
    actor2_name,
    event_code,
    goldstein_scale,
    avg_tone,
    num_mentions,
    num_sources,
    num_articles,
    processed_at

FROM
    events
WHERE
    event_date IS NOT NULL

{% if is_incremental() %}
  -- 이 모델이 이미 데이터를 가지고 있다면,
  -- 최신 날짜보다 더 새로운 데이터만 처리 (3일 버퍼 포함)
    AND event_date >= (SELECT date_add(MAX(event_date), -3) FROM {{ this }})

{% endif %}