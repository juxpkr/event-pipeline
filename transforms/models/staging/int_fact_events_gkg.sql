-- [Intermediate 테이블] : models/staging/int_fact_events_gkg.sql
-- 이벤트, GKG 컨텍스트, 스토리 컬럼 생성

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

WITH stg_events_mapped AS (
    -- 모든 코드가 1차 매핑된 staging 모델
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

stg_detailed AS (
    -- GKG 및 Mentions 상세 정보를 조인한 staging 모델
    SELECT * FROM {{ ref('stg_gkg_detailed_events') }}
),

stg_actor_info AS (
    -- Actor 코드를 재구성한 staging 모델
    SELECT * FROM {{ ref('stg_actors_description') }}
)

SELECT
    -- stg_events_mapped의 모든 컬럼
    stg_events.*,
    
    -- stg_actor_info의 행위자 정보
    stg_actor_info.actor1_info,
    stg_actor_info.actor2_info,

    -- stg_detailed의 컬럼 추가(조인 키인 global_event_id는 제외)
    stg_detailed.mention_source_name,
    stg_detailed.mention_doc_tone,
    stg_detailed.v2_persons,
    stg_detailed.v2_organizations,
    stg_detailed.v2_enhanced_themes,
    stg_detailed.amounts,

    -- 스토리 컬럼 생성
    -- 1. Simple Story: 간단 설명
    stg_events.actor1_name || '이(가) ' || stg_events.actor2_name || '에게 ' || stg_events.mp_event_info || '을(를) 했습니다.' AS simple_story,

    -- 2. Rich Story: 풍부한 설명 (GKG 정보 활용)
    stg_events.mp_actor1_from_country_kor || '의 ' || stg_events.actor1_name || '이(가) ' || stg_events.mp_actor2_from_country_kor || '의 ' || stg_events.actor2_name || '와(과) '
    || stg_events.mp_action_geo_country_kor || ' ' || stg_events.action_geo_fullname || '에서 ' || stg_events.mp_event_info || ' 관련 논의를 했습니다. '
    || '(주요 인물: ' || COALESCE(stg_detailed.v2_persons, '정보 없음') || ', 관련 기관: ' || COALESCE(stg_detailed.v2_organizations, '정보 없음') 
    || ', 주요 테마: ' || COALESCE(stg_detailed.v2_enhanced_themes, '정보 없음') || ')' AS rich_story,

    -- 3. Headline Story: 짧은 헤드라인
    stg_events.mp_action_geo_country_kor || '에서 ' || stg_events.actor1_name || '와(과) ' || stg_events.actor2_name || ' 간 ' || stg_events.mp_event_categories || ' 발생' AS headline_story,

    -- 4. Event Summary: Root Code 기반 카테고리 요약
    stg_events.mp_event_categories || ' (' || stg_events.mp_quad_class || ')' AS event_summary,

    -- 5. Tone Story: 톤 반영된 요약
    CASE
        WHEN stg_events.avg_tone > 2.5 THEN '긍정적 분위기 속에서, '
        WHEN stg_events.avg_tone < -2.5 THEN '부정적 분위기 속에서, '
        ELSE '중립적 분위기 속에서, '
    END || stg_events.actor1_name || '의 ' || stg_events.mp_event_info || ' 이벤트가 발생했습니다.' AS tone_story

FROM
    stg_events_mapped AS stg_events

-- global_event_id를 기준으로 LEFT JOIN 합니다.
LEFT JOIN stg_detailed ON stg_events.global_event_id = stg_detailed.global_event_id
LEFT JOIN stg_actor_info ON stg_events.global_event_id = stg_actor_info.global_event_id

{% if is_incremental() %}
WHERE
    -- 이 모델이 이미 데이터를 가지고 있다면, 최신 날짜보다 더 새로운 데이터만 처리
    stg_events.processed_at > (SELECT MAX(stg_events.processed_at) FROM {{ this }})
{% endif %}