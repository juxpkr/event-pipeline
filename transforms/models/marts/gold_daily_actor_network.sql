-- [Marts 테이블]: models/marts/gold_daily_actor_network.sql
-- Version: 1.0
-- 역할: '국가 간 연결망' 시각화를 위해 '일일 상위 100개' 경로만 필터링하고, 시각화용 컬럼을 추가한 테이블
--       Actor1과 Actor2의 지리 정보가 모두 있는 이벤트만 집계합니다.
-- 실행 주기: 15분 증분

{{ config(
    materialized='incremental',
    unique_key=['event_date', 'actor1_country_iso', 'actor2_country_iso']
) }}

-- CTE 1: 새로 들어온 데이터 중, 국가 간 연결망 분석에 필요한 데이터만 선택
WITH new_events AS (
    SELECT
        event_date,
        mp_actor1_geo_country_iso,
        mp_actor1_geo_country_eng,
        mp_actor1_from_country_kor,
        actor1_geo_lat,
        actor1_geo_long,
        mp_actor2_geo_country_iso,
        mp_actor2_geo_country_eng,
        mp_actor2_from_country_kor,
        actor2_geo_lat,
        actor2_geo_long,
        avg_tone,
        processed_at
    FROM {{ ref('gold_daily_detailed_events') }}
    WHERE 
        mp_actor1_geo_country_iso IS NOT NULL AND mp_actor2_geo_country_iso IS NOT NULL
        AND actor1_geo_lat IS NOT NULL AND actor1_geo_long IS NOT NULL
        AND actor2_geo_lat IS NOT NULL AND actor2_geo_long IS NOT NULL
        AND mp_actor1_geo_country_iso != mp_actor2_geo_country_iso

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
),

-- CTE 2: 새로 들어온 데이터를 '국가 쌍' 기준으로 그룹화하고 집계
daily_aggregates AS (
    SELECT
        event_date,
        mp_actor1_geo_country_iso AS actor1_country_iso,
        mp_actor1_geo_country_eng AS actor1_country_eng,
        mp_actor1_from_country_kor AS actor1_country_kor,
        actor1_geo_lat,
        actor1_geo_long,
        mp_actor2_geo_country_iso AS actor2_country_iso,
        mp_actor2_geo_country_eng AS actor2_country_eng,
        mp_actor2_from_country_kor AS actor2_country_kor,
        actor2_geo_lat,
        actor2_geo_long,
        COUNT(*) AS event_count,
        AVG(avg_tone) AS avg_tone,
        MAX(processed_at) AS processed_at
    FROM new_events
    -- 컬럼 이름이 너무 많고 길 경우, 타이핑을 줄이기 위해 SELECT 절에 명시된 컬럼의 순서를 숫자로 대신 사용
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

-- [신규] CTE 3: TOPN 로직 구현 - 날짜별로 이벤트 수가 많은 상위 100개의 경로에 순위를 매김
ranked_daily_routes AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY event_count DESC) as daily_rank
    FROM daily_aggregates
)

-- 최종 SELECT: 상위 100개 경로만 선택하고, BI 시각화에 필요한 컬럼들을 추가합니다.
SELECT
    event_date,
    actor1_country_iso,
    actor1_country_eng,
    actor1_country_kor,
    actor1_geo_lat,
    actor1_geo_long,
    actor2_country_iso,
    actor2_country_eng,
    actor2_country_kor,
    actor2_geo_lat,
    actor2_geo_long,
    event_count,
    avg_tone,
    processed_at,
    
    -- [신규] DAX의 ADDCOLUMNS 로직 구현
    -- 1. Location 컬럼 (루트맵 시각화를 위한 좌표 텍스트)
    '[' || format_string('%.6f', actor1_geo_lat) || '|' || format_string('%.6f', actor1_geo_long) || '] ' || 
    '[' || format_string('%.6f', actor2_geo_lat) || '|' || format_string('%.6f', actor2_geo_long) || ']' AS location_text,

    -- 2. Name 컬럼 (경로 이름)
    actor1_country_iso || ' → ' || actor2_country_iso AS route_name,

    -- 3. ToneCategory 컬럼 (평균 톤을 긍정/부정/중립으로 분류)
    CASE
        WHEN avg_tone > 0 THEN 'Positive'
        WHEN avg_tone < 0 THEN 'Negative'
        ELSE 'Neutral'
    END AS tone_category

FROM ranked_daily_routes
WHERE daily_rank <= 100