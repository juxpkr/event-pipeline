-- [Staging Layer] : models/staging/stg_gkg_detailed_events.sql
-- Version : 2.0
-- GKG 및 Mentions 상세 정보 정제
-- View로 작동하므로, 증분 관련 코드를 제거함

{{ config(materialized='view')}}

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

