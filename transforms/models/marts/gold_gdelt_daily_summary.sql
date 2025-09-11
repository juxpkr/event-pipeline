-- models/marts/gold_gdelt_daily_summary.sql
-- 수정 필요

-- 1. Staging 모델을 CTE로 정의합니다.
WITH events_mapped AS (
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

-- 2. 일별/국가별 기사 수를 계산하여 순위를 매기는 CTE를 만듭니다.
daily_country_articles AS (
    SELECT
        event_date,
        action_geo_country,
        SUM(num_articles) AS total_articles,
        -- 각 날짜 안에서 기사 수 합계가 가장 높은 국가를 찾기 위해 순위(rank)를 매깁니다.
        ROW_NUMBER() OVER(PARTITION BY event_date ORDER BY SUM(num_articles) DESC) as country_rank
    FROM
        events_mapped
    WHERE
        action_geo_country IS NOT NULL
    GROUP BY
        event_date,
        action_geo_country
),

top_countries AS (
    SELECT
        event_date,
        action_geo_country AS top_country_name,
        total_articles AS top_country_articles
    FROM
        daily_country_articles
    WHERE
        country_rank = 1 -- 기사 수가 1위인 국가만 선택
),

-- 3. 일별로 모든 지표를 집계하는 CTE를 만듭니다.
daily_summary AS (
    SELECT
        event_date,
        COUNT(*) AS events_total,
        SUM(num_articles) AS articles_total,
        SUM(num_sources) AS sources_total,
        SUM(num_mentions) AS mentions_total,
        AVG(avg_tone) AS avg_tone_mean,
        APPROX_PERCENTILE(avg_tone, 0.5) AS avg_tone_median,
        AVG(goldstein_scale) AS goldstein_mean,
        AVG(ABS(goldstein_scale)) AS goldstein_abs_mean,
        AVG(CASE WHEN avg_tone < 0 THEN 1.0 ELSE 0.0 END) AS neg_ratio,
        COUNT(CASE WHEN quad_class = 1 THEN 1 END) AS quad1_cnt,
        COUNT(CASE WHEN quad_class = 2 THEN 1 END) AS quad2_cnt,
        COUNT(CASE WHEN quad_class = 3 THEN 1 END) AS quad3_cnt,
        COUNT(CASE WHEN quad_class = 4 THEN 1 END) AS quad4_cnt,

        -- 데이터 품질(DQ) 지표 계산
        AVG(CASE WHEN regexp_like(action_geo_country_code, '^[A-Z]{2}$') THEN 1.0 ELSE 0.0 END) AS dq_action_geo_alpha_ratio,
        AVG(CASE WHEN regexp_like(action_geo_country_code, '^[0-9]+$') THEN 1.0 ELSE 0.0 END) AS dq_action_geo_numeric_ratio,
        AVG(CASE WHEN LENGTH(actor1_country_code) = 3 THEN 1.0 ELSE 0.0 END) AS dq_actor1_iso3_ratio,
        AVG(CASE WHEN action_geo_lat IS NOT NULL AND action_geo_long IS NOT NULL THEN 1.0 ELSE 0.0 END) AS dq_latlong_coverage

    FROM
        events_mapped
    WHERE
        event_date IS NOT NULL
    GROUP BY
        event_date
)

-- 4. 집계된 일별 요약과 TOP 국가 정보를 최종적으로 JOIN합니다.
SELECT
    ds.event_date,
    ds.events_total,
    ds.articles_total,
    ds.sources_total,
    ds.mentions_total,
    ds.avg_tone_mean,
    ds.avg_tone_median,
    ds.goldstein_mean,
    ds.goldstein_abs_mean,
    ds.neg_ratio,
    ds.quad1_cnt,
    ds.quad2_cnt,
    ds.quad3_cnt,
    ds.quad4_cnt,
    tc.top_country_name,
    tc.top_country_articles,
    ds.dq_action_geo_alpha_ratio,
    ds.dq_action_geo_numeric_ratio,
    ds.dq_actor1_iso3_ratio,
    ds.dq_latlong_coverage,
    CURRENT_TIMESTAMP() AS updated_at

FROM
    daily_summary ds
LEFT JOIN
    top_countries tc ON ds.event_date = tc.event_date
ORDER BY
    ds.event_date DESC