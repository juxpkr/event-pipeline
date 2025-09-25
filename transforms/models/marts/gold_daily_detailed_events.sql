-- [Marts 테이블]: models/marts/gold_daily_detailed_events.sql
-- Version : 1.0
-- 역할: Staging 모델들을 결합하고, 스토리 및 KPI 파생 컬럼을 생성하는 상세 이벤트 테이블
-- 실행 주기: Airflow에서 하루에 한 번 dbt run --select gold_daily_detailed_events를 실행합니다.

{{ config(
    materialized='incremental',
    unique_key='global_event_id'
) }}

-- 1. CTE: 필요한 Staging 모델들을 각각 정의합니다.
WITH stg_events_mapped AS (
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

stg_detailed AS (
    SELECT * FROM {{ ref('stg_gkg_detailed_events') }}
),

stg_actors_description AS (
    SELECT * FROM {{ ref('stg_actors_description') }}
),

-- 2. CTE: 모든 소스를 조인하여 분석의 기반이 될 데이터를 만듭니다.
joined_sources AS (
    SELECT
        stg_events.*,
        stg_actor1_info.actor_full_description AS actor1_info,
        stg_actor2_info.actor_full_description AS actor2_info,
        stg_detailed.mention_source_name,
        stg_detailed.mention_doc_tone,
        stg_detailed.v2_persons,
        stg_detailed.v2_organizations,
        stg_detailed.v2_enhanced_themes,
        stg_detailed.amounts
    FROM stg_events_mapped AS stg_events
    LEFT JOIN stg_actors_description AS stg_actor1_info ON stg_events.actor1_code = stg_actor1_info.actor_code
    LEFT JOIN stg_actors_description AS stg_actor2_info ON stg_events.actor2_code = stg_actor2_info.actor_code
    LEFT JOIN stg_detailed ON stg_events.global_event_id = stg_detailed.global_event_id
),

-- 3. CTE: Window Function을 사용하여 분석 컬럼을 생성합니다.
analytics_calculations AS (
    SELECT
        *,
        (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) AS goldstein_zscore,
        (avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0) AS tone_zscore
    FROM joined_sources
)

-- 4. 최종 SELECT: 모든 파생 컬럼과 스토리 컬럼을 생성합니다.
SELECT
    *,
    CASE
        WHEN goldstein_zscore > 2 OR tone_zscore < -2 OR tone_zscore > 2 THEN true
        ELSE false
    END AS is_anomaly,
    
    CASE 
        WHEN quad_class IN (1, 2) THEN '협력'
        WHEN quad_class IN (3, 4) THEN '갈등'
        ELSE '중립'
    END AS event_type,

    (
        -0.5 * IFNULL(goldstein_scale, 0) +
        -0.3 * IFNULL(avg_tone, 0) +
        0.2 * LN(IFNULL(num_articles, 1))   -- num_articles가 0 또는 NULL일 경우를 대비해 1로 처리
    ) AS risk_score_detailed,

    actor1_name || '이(가) ' || actor2_name || '에게 ' || mp_event_info || '을(를) 했습니다.' AS simple_story,
    actor1_info || '(' || actor1_name || ')' || '이(가) ' || actor2_info || '(' || actor2_name || ')' || '에게 ' || mp_event_info || '을(를) 했습니다.' AS simple_story_v2,

    mp_actor1_from_country_kor || '의 ' || actor1_name || '이(가) ' || mp_actor2_from_country_kor || '의 ' || actor2_name || '와(과) '
    || mp_action_geo_country_kor || '에서 ' || mp_event_info || ' 관련 논의를 했습니다.'
    || '(주요 인물: ' || COALESCE(v2_persons, '정보 없음') || ', 관련 기관: ' || COALESCE(v2_organizations, '정보 없음') 
    || ', 주요 테마: ' || COALESCE(v2_enhanced_themes, '정보 없음') || ')' AS rich_story,

    mp_action_geo_country_kor || '에서 ' || actor1_name || '와(과) ' || actor2_name || ' 간 ' || mp_event_categories || ' 발생' AS headline_story,

    mp_event_categories || ' (' || mp_quad_class || ')' AS event_summary,
    
    CASE
        WHEN avg_tone > 2.5 THEN '긍정적 분위기 속에서, '
        WHEN avg_tone < -2.5 THEN '부정적 분위기 속에서, '
        ELSE '중립적 분위기 속에서, '
    END || actor1_name || '의 ' || mp_event_info || ' 이벤트가 발생했습니다.' AS tone_story

FROM analytics_calculations

-- {% if is_incremental() %}
-- WHERE processed_at > (SELECT MAX(processed_at) FROM {{ this }})
-- {% endif %}

{% if is_incremental() %}
-- run_query 매크로를 사용해, 대상 테이블의 max(processed_at) 값을 먼저 조회해서 변수에 저장한다.
{% set max_processed_at = run_query("SELECT max(processed_at) FROM " ~ this).columns[0].values()[0] %}
-- 이 모델이 이미 데이터를 가지고 있다면, 최신 날짜보다 더 새로운 데이터만 처리
WHERE processed_at > '{{ max_processed_at }}'
{% endif %}