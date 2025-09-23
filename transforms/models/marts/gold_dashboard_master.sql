-- [Marts 테이블] : models/marts/gold_dashboard_master.sql
-- Superset 연결용 최종 마스터 테이블

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

WITH events_master AS (
    -- Intermediate 테이블
    SELECT * FROM {{ ref('int_fact_events_gkg') }}
)

SELECT
    -- 그룹화 기준(대시보드 필터용)
    event_date,
    mp_event_categories AS event_category,

    mp_actor1_geo_country_iso AS actor1_geo_iso,
    mp_actor1_geo_country_eng AS actor1_geo_eng,
    mp_actor1_geo_country_kor AS actor1_geo_kor,
    actor1_geo_lat,
    actor1_geo_long,

    mp_actor2_geo_country_iso AS actor2_geo_iso,
    mp_actor2_geo_country_eng AS actor2_geo_eng,
    mp_actor2_geo_country_kor AS actor2_geo_kor,
    actor2_geo_lat,
    actor2_geo_long,

    mp_action_geo_country_iso AS action_geo_iso,
    mp_action_geo_country_eng AS action_geo_eng,
    mp_action_geo_country_kor AS action_geo_kor,
    action_geo_lat,
    action_geo_long,

    -- 집계 지표(KPI 및 차트용)
    COUNT(*) AS event_count,
    AVG(goldstein_scale) AS avg_goldstein_scale,
    AVG(avg_tone) AS avg_tone,
    SUM(num_mentions) AS total_mentions,
    SUM(num_sources) AS total_sources,
    SUM(num_articles) AS total_articles,

    -- 어떤 종류의 이벤트가 주로 발생했는지 파악
    COUNT(CASE WHEN quad_class = 1 THEN 1 END) AS count_class1,
    COUNT(CASE WHEN quad_class = 2 THEN 1 END) AS count_class2,
    COUNT(CASE WHEN quad_class = 3 THEN 1 END) AS count_class3,
    COUNT(CASE WHEN quad_class = 4 THEN 1 END) AS count_class4,
    
    -- 협력/갈등 이벤트 카운트
    COUNT(CASE WHEN quad_class IN (1, 2) THEN 1 END) AS count_cooperation_event,
    COUNT(CASE WHEN quad_class IN (3, 4) THEN 1 END) AS count_conflict_event,

    -- Risk Score 계산
    (
        -0.5 * IFNULL(AVG(goldstein_scale), 0) +
        -0.3 * IFNULL(AVG(avg_tone), 0) +
        0.2 * LN(COUNT(*))
    ) AS risk_score,
    
    CURRENT_TIMESTAMP() AS updated_at   -- SQL문이 실행된 시점

FROM
    events_master

WHERE
    event_date IS NOT NULL
    -- Actor1과 Actor2가 모두 국가 단위이고, 서로 다른 국가일 때의 이벤트만 필터링

    {% if is_incremental() %}
        -- run_query 매크로를 사용해, 대상 테이블의 max(updated_at) 값을 먼저 조회해서 변수에 저장한다.
      {% set max_updated_at = run_query("SELECT max(updated_at) FROM " ~ this).columns[0].values()[0] %}

        -- 이 모델이 이미 데이터를 가지고 있다면, 최신 날짜보다 더 새로운 데이터만 처리 (소스 데이터 기준)
        AND processed_at > '{{ max_updated_at }}'
    {% endif %}

GROUP BY
    event_date,
    event_category,
    actor1_geo_iso,
    actor1_geo_eng,
    actor1_geo_kor,
    actor1_geo_lat,
    actor1_geo_long,
    actor2_geo_iso,
    actor2_geo_eng,
    actor2_geo_kor,
    actor2_geo_lat,
    actor2_geo_long,
    action_geo_iso,
    action_geo_eng,
    action_geo_kor,
    action_geo_lat,
    action_geo_long

ORDER BY
    event_date DESC,
    mp_action_geo_country_eng