-- models/marts/gold_1st_global_overview.sql
-- [골드 테이블] 전세계 레벨 - 국가별 & 일일 요약

WITH events AS (
    SELECT * FROM {{ ref('stg_seed_mapping') }}
)

SELECT
    event_date,
<<<<<<< HEAD
    mp_action_geo_country_iso AS action_geo_iso,
    mp_action_geo_country_eng AS action_geo_name,
=======
    mp_action_location_country AS country_name,
>>>>>>> develop

    -- Risk Score 계산 (추천식)
    -- 설명: 갈등 이벤트의 강도(goldstein)와 양(event count), 그리고 부정적인 언론 톤을 조합하여 위험도를 계산합니다.
    -- 정규화(Normalization)를 통해 각 지표의 스케일을 맞추고 가중치를 부여하여 종합 점수를 산출합니다.
    (
        -0.5 * IFNULL(AVG(goldstein_scale), 0) +        -- 갈등 강도 (낮을수록 위험)
        -0.3 * IFNULL(AVG(avg_tone), 0) +               -- 부정적 언론 톤 (낮을수록 위험)
        0.2 * LN(COUNT(*))                              -- 이벤트 발생량 (많을수록 위험, 로그 스케일 적용)
    ) AS risk_score,

    -- 기본 집계 지표
    AVG(goldstein_scale) AS avg_goldstein_scale,
    SUM(num_mentions) AS total_num_mentions,
    SUM(num_sources) AS total_num_sources,
    SUM(num_articles) AS total_num_articles,
    AVG(avg_tone) AS avg_tone,
    COUNT(*) as event_count,

    CURRENT_TIMESTAMP() AS updated_at       -- SQL문이 실행된 시점

FROM
    events
WHERE
    event_date IS NOT NULL
<<<<<<< HEAD
    AND mp_action_geo_country_eng IS NOT NULL
GROUP BY
    event_date,
    action_geo_iso,
    action_geo_name
ORDER BY
    event_date DESC,
    action_geo_name
=======
    AND mp_action_location_country IS NOT NULL
GROUP BY
    event_date,
    mp_action_location_country
ORDER BY
    event_date DESC,
    country_name
>>>>>>> develop
