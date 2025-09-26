-- [Marts 테이블]: models/marts/gold_daily_events_category.sql
-- Version: 1.0
-- 역할: 이벤트 타입(4대 분류) 및 카테고리별 이벤트 수를 모두 집계하는 테이블
-- 실행 주기: 15분 증분

{{ config(
    materialized='incremental',
    unique_key=['event_date', 'mp_action_geo_country_iso', 'mp_event_categories']
) }}

-- CTE 1: 15분마다 새로 들어온 유효한 데이터만 선택
WITH new_events AS (
    SELECT
        event_date,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        mp_quad_class,
        mp_event_categories,
        processed_at
    FROM {{ ref('stg_seed_mapping') }}
    WHERE 
        mp_action_geo_country_iso IS NOT NULL
        AND mp_quad_class IS NOT NULL
        AND mp_event_categories IS NOT NULL

    {% if is_incremental() %}
    -- run_query 매크로를 사용해, 대상 테이블의 max(processed_at) 값을 먼저 조회해서 변수에 저장합니다.
    {% set max_query %}
        SELECT MAX(processed_at) FROM {{ this }}
    {% endset %}
    {% set result = run_query(max_query) %}
    
    {% if execute and result.rows and result.rows[0][0] is not none %}
        {% set max_processed_at = result.rows[0][0] %}
    {% else %}
        -- 만약 테이블이 비어있거나, 모든 값이 NULL이면 프로젝트 시작 날짜를 기본값으로 사용합니다.
        {% set max_processed_at = '2023-09-01 00:00:00' %}
    {% endif %}

    AND processed_at > '{{ max_processed_at }}'
    {% endif %}
)

-- 최종 SELECT: 새로 들어온 데이터를 가장 상세한 레벨로 그룹화하고 집계합니다.
SELECT
    -- 집계의 기준이 되는 차원(Dimension) 컬럼들
    event_date,
    mp_action_geo_country_iso,
    mp_action_geo_country_eng,
    mp_action_geo_country_kor,
    mp_quad_class,
    mp_event_categories,
    
    -- 집계된 측정값(Metric)
    COUNT(*) AS event_count,
    
    -- 증분 처리를 위한 책갈피
    MAX(processed_at) AS processed_at

FROM
    new_events
    
GROUP BY
    event_date,
    mp_action_geo_country_iso,
    mp_action_geo_country_eng,
    mp_action_geo_country_kor,
    mp_quad_class,
    mp_event_categories