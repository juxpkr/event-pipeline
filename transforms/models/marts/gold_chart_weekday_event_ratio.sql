-- [Marts 테이블]: models/marts/gold_chart_weekday_event_ratio.sql
-- Version: 1.0
-- 역할: '요일별 이벤트 발생 비율' 꺾은선 비교 차트를 위한 집계 테이블
-- 증분 방식으로 만들 경우, 계산 결과가 잘못되기 때문에 의도적으로 테이블(table)로 만든 것
-- '총 이벤트 수'(분모)를 정확하게 구하려면, dbt는 항상 전체 데이터를 보고 다시 계산해야 함

{{ config(
    materialized='table'
) }}

-- CTE 1: 분석에 필요한 기본 데이터 필터링 및 요일 정보 추가
WITH base_events AS (
    SELECT
        event_date,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        -- Spark SQL에서 요일 추출 (1=일요일, 2=월요일, ..., 7=토요일)
        DAYOFWEEK(event_date) AS weekday_num,
        -- 알아보기 쉽도록 요일 이름도 추가 (예: Sun, Mon)
        DATE_FORMAT(event_date, 'E') AS weekday_name
    FROM {{ ref('stg_seed_mapping') }}
    WHERE 
        mp_action_geo_country_iso IS NOT NULL
),

-- CTE 2: '지난 2년' 기간의 요일별 이벤트 수 및 비율 계산
agg_last_2_years AS (
    SELECT
        '지난 2년' AS period,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        weekday_num,
        weekday_name,
        -- 각 국가별 총 이벤트 수를 분모로 사용하여 비율 계산
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY mp_action_geo_country_iso) AS event_percentage
    FROM base_events
    GROUP BY
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        weekday_num,
        weekday_name
),

-- CTE 3: '이번 달' (2025년 9월) 기간의 요일별 이벤트 수 및 비율 계산
agg_current_month AS (
    SELECT
        '이번 달 (25년 9월)' AS period,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        weekday_num,
        weekday_name,
        -- 각 국가별/이번 달 총 이벤트 수를 분모로 사용하여 비율 계산
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY mp_action_geo_country_iso) AS event_percentage
    FROM base_events
    WHERE
        -- 2025년 9월 데이터만 필터링
        YEAR(event_date) = 2025 AND MONTH(event_date) = 9
    GROUP BY
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        weekday_num,
        weekday_name
)

-- 최종 SELECT: 두 기간의 결과를 하나의 테이블로 합칩니다.
SELECT *
FROM agg_last_2_years

UNION ALL

SELECT *
FROM agg_current_month