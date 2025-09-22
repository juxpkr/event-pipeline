-- models/marts/gold_2nd_country_events.sql
-- [골드 테이블] 국가 레벨

WITH events AS (
    SELECT * FROM {{ ref('stg_seed_mapping') }}
)

SELECT
    event_date,

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

    -- 기본 집계 지표
    AVG(goldstein_scale) AS avg_goldstein_scale,
    SUM(num_mentions) AS total_mentions,
    SUM(num_sources) AS total_sources,
    SUM(num_articles) AS total_articles,
    AVG(avg_tone) AS avg_tone,
    COUNT(*) AS count_event,

    -- (추천) 추가 정보: 어떤 종류의 이벤트가 주로 발생했는지 파악
    COUNT(CASE WHEN quad_class = 1 THEN 1 END) AS count_class1,
    COUNT(CASE WHEN quad_class = 2 THEN 1 END) AS count_class2,
    COUNT(CASE WHEN quad_class = 3 THEN 1 END) AS count_class3,
    COUNT(CASE WHEN quad_class = 4 THEN 1 END) AS count_class4,
    
    CURRENT_TIMESTAMP() AS updated_at   -- SQL문이 실행된 시점

FROM
    events
WHERE
    -- Actor1과 Actor2가 모두 국가 단위이고, 서로 다른 국가일 때의 이벤트만 필터링
    mp_actor1_geo_country_eng IS NOT NULL
    AND mp_actor2_geo_country_eng IS NOT NULL
    AND mp_actor1_geo_country_eng != mp_actor2_geo_country_eng
GROUP BY
    event_date,
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
    count_event DESC