-- [Staging 테이블]: models/staging/stg_gkg_detailed_events.sql
-- GKG 및 Mentions 상세 정보 정제

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('gdelt_silver_layer', 'gdelt_events_detailed') }}
)

SELECT
    -- Events 정보
    global_event_id,
    event_date,
    source_url,

    -- Mentions 정보
    mention_source_name,
    mention_doc_tone,

    -- GKG 정보
    v2_persons,
    v2_organizations,
    v2_enhanced_themes,
    amounts,
    processed_at

FROM
    source_data

{% if is_incremental() %}
WHERE
    -- 이 모델이 이미 데이터를 가지고 있다면, 최신 날짜보다 더 새로운 데이터만 처리
    processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}