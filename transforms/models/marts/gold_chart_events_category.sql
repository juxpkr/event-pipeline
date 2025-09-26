-- [Marts 테이블]: models/marts/gold_chart_events_category.sql
-- Version: 1.0
-- 역할: 이벤트 타입(4대 분류) 및 카테고리별 이벤트 수를 집계하는 테이블
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
    {% set max_query %}
        SELECT MAX(processed_at) FROM {{ this }}
    {% endset %}
    {% set result = run_query(max_query) %}
    
    {% if execute and result.rows and result.rows[0][0] is not none %}
        {% set max_processed_at = result.rows[0][0] %}
    {% else %}
        {% set max_processed_at = '2023-09-01 00:00:00' %}
    {% endif %}

    AND processed_at > '{{ max_processed_at }}'
    {% endif %}
)

-- 최종 SELECT
SELECT
    event_date,
    mp_action_geo_country_iso,
    mp_action_geo_country_eng,
    mp_action_geo_country_kor,
    mp_quad_class,
    mp_event_categories,
    COUNT(*) AS event_count,
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