-- models/marts/gold_2nd_country_events.sql
-- [골드 테이블] 국가 레벨

-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['event_date', 'actor1_country', 'actor2_country']
) }}

WITH events AS (
    -- 증분 처리를 위해 매핑된 Silver 데이터 참조
    SELECT * FROM {{ ref('stg_seed_mapping') }}
)

SELECT
    event_date,
    mp_actor1_affiliation_country AS actor1_country,
    mp_actor2_affiliation_country AS actor2_country,

    -- 기본 집계 지표
    AVG(goldstein_scale) AS avg_goldstein_scale,
    SUM(num_mentions) AS total_mentions,
    SUM(num_sources) AS total_sources,
    SUM(num_articles) AS total_articles,
    AVG(avg_tone) AS avg_tone,
    COUNT(*) AS event_count,

    -- (추천) 추가 정보: 어떤 종류의 이벤트가 주로 발생했는지 파악
    COUNT(CASE WHEN mp_quad_class = 'Verbal Cooperation' THEN 1 END) AS verbal_coop_count,
    COUNT(CASE WHEN mp_quad_class = 'Material Cooperation' THEN 1 END) AS material_coop_count,
    COUNT(CASE WHEN mp_quad_class = 'Verbal Conflict' THEN 1 END) AS verbal_conflict_count,
    COUNT(CASE WHEN mp_quad_class = 'Material Conflict' THEN 1 END) AS material_conflict_count,
    
    CURRENT_TIMESTAMP() AS updated_at       -- SQL문이 실행된 시점

FROM
    events
WHERE
    -- Actor1과 Actor2가 모두 국가 단위이고, 서로 다른 국가일 때의 이벤트만 필터링
    mp_actor1_affiliation_country IS NOT NULL
    AND mp_actor2_affiliation_country IS NOT NULL
    AND mp_actor1_affiliation_country != mp_actor2_affiliation_country
GROUP BY
    event_date,
    actor1_country,
    actor2_country
ORDER BY
    event_date DESC,
    event_count DESC

{% if is_incremental() and adapter.get_relation(this.database, this.schema, this.identifier) %}

  -- 이 모델이 이미 데이터를 가지고 있다면,
  -- 최신 날짜보다 더 새로운 데이터만 처리 (3일 버퍼 포함)
  AND event_date >= date_add((SELECT MAX(event_date) FROM {{ this }}), -3)

{% endif %}