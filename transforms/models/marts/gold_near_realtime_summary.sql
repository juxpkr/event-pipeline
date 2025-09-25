-- [Marts 테이블]: models/marts/gold_near_realtime_summary.sql
-- Version : 2.0
-- 역할: 15분마다 업데이트되는 gdelt_events 데이터만을 사용하여, 대시보드의 핵심 KPI(Risk Score, 이벤트 수 등)를 빠르게 집계합니다. GKG & Mentions 정보는 포함하지 않습니다.
-- 실행 주기: Airflow에서 15분마다 dbt run --select gold_near_realtime_summary를 실행합니다.

{{ config(
    materialized='incremental',
    unique_key=['event_date', 'mp_action_geo_country_iso']
) }}

-- CTE 1: 새로 들어온 데이터만 선택
WITH new_events AS (
    SELECT *
    FROM {{ ref('stg_seed_mapping') }}

    -- {% if is_incremental() %}
    -- -- run_query 매크로를 사용해, 대상 테이블의 max(processed_at) 값을 먼저 조회해서 변수에 저장한다.
    -- {% set max_processed_at = run_query("SELECT max(processed_at) FROM " ~ this).columns[0].values()[0] %}
    -- -- 이 모델이 이미 데이터를 가지고 있다면, 최신 날짜보다 더 새로운 데이터만 처리
    -- WHERE processed_at > '{{ max_processed_at }}'
    -- {% endif %}

    {% if is_incremental() %}
    -- [수정] run_query 결과가 NULL일 경우를 대비한 안전장치를 추가합니다.
    {% set max_query %}
        SELECT MAX(processed_at) FROM {{ this }}
    {% endset %}
    {% set result = run_query(max_query) %}
    
    {% if execute and result.rows and result.rows[0][0] is not none %}
        {% set max_processed_at = result.rows[0][0] %}
    {% else %}
        -- 만약 테이블이 비어있거나, 모든 값이 NULL이면 아주 오래된 날짜를 기본값으로 사용합니다.
        {% set max_processed_at = '2023-09-01 00:00:00' %}
    {% endif %}

    WHERE processed_at > '{{ max_processed_at }}'
    {% endif %}
),

-- {% if is_incremental() %}
-- -- CTE 2: (증분 시에만 실행) Z-Score 계산에 필요한 과거 데이터 선택(최근 30일)
-- historical_for_zscore AS (
--     SELECT global_event_id, goldstein_scale, avg_tone, event_date, processed_at
--     FROM {{ this }}   -- 자기 자신(이미 만들어진 gold 테이블)을 참조
--     WHERE event_date >= DATE_SUB((SELECT MAX(event_date) FROM new_events), 30)
-- ),

-- -- CTE 3: (증분 시에만 실행) 신규 + 과거 데이터를 합쳐 Z-Score 계산용 데이터셋 생성
-- unioned_for_zscore AS (
--     SELECT global_event_id, goldstein_scale, avg_tone FROM new_events
--     UNION ALL
--     SELECT global_event_id, goldstein_scale, avg_tone FROM historical_for_zscore
-- ),
-- {% endif %}

-- -- CTE 4: Z-Score 계산
-- z_score_calculation AS (
--     SELECT
--         global_event_id,
--         -- 증분 시에는 신규+과거 데이터로, 전체 실행 시에는 new_events 전체 데이터로 Z-Score 계산
--         (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) AS goldstein_zscore,
--         (avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0) AS tone_zscore
--     FROM {% if is_incremental() %} unioned_for_zscore {% else %} new_events {% endif %}
-- ),

-- [수정] CTE 2: Z-Score 계산을 'new_events' 범위 내에서만 수행하도록 단순화합니다.
z_score_calculation AS (
    SELECT
        global_event_id,
        (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) AS goldstein_zscore,
        (avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0) AS tone_zscore
    FROM new_events
),

-- CTE 3: 새로 들어온 데이터에 Z-Score를 최종 조인
final_events_to_aggregate AS (
    SELECT
        events.*,
        z_scores.goldstein_zscore,
        z_scores.tone_zscore
    FROM new_events AS events
    LEFT JOIN z_score_calculation AS z_scores ON events.global_event_id = z_scores.global_event_id
),

-- CTE 4: 일일/국가별/카테고리별 이벤트 카운트를 집계합니다.
event_counts_per_day AS (
    SELECT
        event_date,
        mp_action_geo_country_iso,
        mp_event_categories,
        COUNT(*) as category_count
    FROM final_events_to_aggregate
    WHERE mp_action_geo_country_iso IS NOT NULL AND mp_event_categories IS NOT NULL
    GROUP BY 1, 2, 3
),

-- CTE 5: 집계된 카운트를 기준으로 순위를 매깁니다.
ranked_events_per_day AS (
    SELECT
        event_date,
        mp_action_geo_country_iso,
        mp_event_categories,
        ROW_NUMBER() OVER (PARTITION BY event_date, mp_action_geo_country_iso ORDER BY category_count DESC) as rn
    FROM event_counts_per_day
),

-- CTE 6: 기본 집계 수행
aggregated_summary AS (
    SELECT
        event_date,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor,
        COUNT(*) AS event_count,
        AVG(goldstein_scale) AS avg_goldstein_scale,
        AVG(avg_tone) AS avg_tone,
        SUM(num_mentions) AS total_mentions,
        SUM(num_sources) AS total_sources,
        SUM(num_articles) AS total_articles,
        COUNT(CASE WHEN quad_class IN (1, 2) THEN 1 END) AS count_cooperation_event,
        COUNT(CASE WHEN quad_class IN (3, 4) THEN 1 END) AS count_conflict_event,
        COUNT(CASE WHEN (goldstein_zscore > 2 OR tone_zscore < -2 OR tone_zscore > 2) THEN 1 END) AS count_anomaly_event,
        (
            -0.5 * IFNULL(AVG(goldstein_scale), 0) +
            -0.3 * IFNULL(AVG(avg_tone), 0) +
            0.2 * LN(COUNT(*))
        ) AS risk_score_daily,
        MAX(processed_at) AS processed_at
    FROM final_events_to_aggregate
    WHERE mp_action_geo_country_iso IS NOT NULL
    GROUP BY
        event_date,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor
)

-- 최종 SELECT: 새로 들어온 데이터만 그룹화하고 집계한 후, 가장 빈번한 이벤트 정보를 조인합니다.
SELECT
    -- 집계의 기준이 되는 컬럼들
    agg.event_date,
    agg.mp_action_geo_country_iso,
    agg.mp_action_geo_country_eng,
    agg.mp_action_geo_country_kor,
    
    -- 기본 집계 지표
    agg.event_count,
    agg.avg_goldstein_scale,
    agg.avg_tone,
    agg.total_mentions,
    agg.total_sources,
    agg.total_articles,
    agg.count_cooperation_event,
    agg.count_conflict_event,
    agg.count_anomaly_event,
    agg.risk_score_daily,
    agg.processed_at,

    -- 가장 빈번한 이벤트 카테고리를 활용한 최종 톤 요약 스토리
    CASE
        WHEN agg.avg_tone > 2.5 THEN agg.mp_action_geo_country_kor || '에서는 "' || freq.mp_event_categories || '" 관련 긍정적인 분위기의 이벤트가 주로 발생했습니다.'
        WHEN agg.avg_tone < -2.5 THEN agg.mp_action_geo_country_kor || '에서는 "' || freq.mp_event_categories || '" 관련 부정적인 분위기의 이벤트가 주로 발생했습니다.'
        ELSE agg.mp_action_geo_country_kor || '에서는 "' || freq.mp_event_categories || '" 관련 중립적인 분위기의 이벤트가 주로 발생했습니다.'
    END AS daily_tone_summary

FROM aggregated_summary AS agg

LEFT JOIN (
    SELECT * FROM ranked_events_per_day WHERE rn = 1
) AS freq
    ON agg.event_date = freq.event_date AND agg.mp_action_geo_country_iso = freq.mp_action_geo_country_iso