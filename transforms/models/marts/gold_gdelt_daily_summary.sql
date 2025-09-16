-- models/marts/gold_gdelt_daily_summary.sql
-- 골드 테이블 일일 요약
-- event_date	                date	    GDELT day(yyyyMMdd)에서 파생한 날짜(일 단위 그레인).
-- events_total                 bigint	    해당 일자의 이벤트 건수(행 수).
-- articles_total	            bigint	    sum(num_articles) — 해당 일자에 수집된 기사 수 합계.
-- sources_total	            bigint	    sum(num_sources) — 출처(소스) 수 합계.
-- mentions_total	            bigint	    sum(num_mentions) — 언급(멘션) 수 합계.
-- avg_tone_mean	            double	    avg(avg_tone) — 이벤트 톤의 평균(음수=부정, 양수=긍정 경향).
-- avg_tone_median	            double	    percentile_approx(avg_tone, 0.5) — 이벤트 톤의 중앙값.
-- goldstein_mean	            double	    avg(goldstein_scale) — 이벤트 영향도 점수의 평균(양/음의 평균).
-- goldstein_abs_mean	        double	    avg(abs(goldstein_scale)) — 영향도의 절대값 평균(강도 중심).
-- neg_ratio	                double	    avg(CASE WHEN avg_tone < 0 THEN 1 ELSE 0 END) — 부정 톤 비율.
-- quad1_cnt	                bigint	    quad_class=1 건수(협력/언어적 협력 등 저강도 협력).
-- quad2_cnt	                bigint	    quad_class=2 건수(물질적 협력 등 고강도 협력).
-- quad3_cnt	                bigint	    quad_class=3 건수(언어적 갈등 등 저강도 갈등).
-- quad4_cnt	                bigint	    quad_class=4 건수(폭력/군사적 충돌 등 고강도 갈등).
-- top_country_iso3	            string	    당일 기사 수 합 기준 TOP 국가 코드.(임시 규칙: action_geo_country_code(FIPS2) 유효 시 우선, 없으면 actor1→actor2; ETL 수정 후 ISO3로 정규화 예정)
-- top_country_articles     	bigint	    해당 TOP 국가의 sum(num_articles) 값.
-- dq_action_geo_alpha_ratio	double	    action_geo_country_code가 알파벳 2자(FIPS) 인 비율(정상 기대치).
-- dq_action_geo_numeric_ratio	double	    action_geo_country_code가 숫자로 들어간 비율(매핑 오류 지표).
-- dq_actor1_iso3_ratio	        double	    actor1_country_code가 3자 코드(CAMEO/ISO3 유사) 인 비율.
-- dq_latlong_coverage          double	    action_geo_lat와 action_geo_long가 둘 다 존재하는 비율.
-- updated_at	                timestamp	해당 일일 요약 레코드의 생성(업데이트) 시각.

-- 1. 최종 Staging 모델을 CTE로 정의합니다.
WITH events_mapped AS (
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

-- 2. 일별/국가별 기사 수를 계산하여 순위를 매기는 CTE를 만듭니다.
daily_country_articles AS (
    SELECT
        event_date,
        mp_action_location_country AS country_name,
        SUM(num_articles) AS total_articles,
        -- 각 날짜 안에서 기사 수 합계가 가장 높은 국가를 찾기 위해 순위(rank)를 매깁니다.
        ROW_NUMBER() OVER(PARTITION BY event_date ORDER BY SUM(num_articles) DESC) as country_rank
    FROM
        events_mapped
    WHERE
        mp_action_location_country IS NOT NULL
    GROUP BY
        event_date,
        mp_action_location_country
),

top_countries AS (
    SELECT
        event_date,
        country_name AS top_country_name,
        total_articles AS top_country_articles
    FROM
        daily_country_articles
    WHERE
        country_rank = 1   -- 기사 수가 1위인 국가만 선택
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
        COUNT(CASE WHEN quad_class = '1' THEN 1 END) AS quad1_cnt,
        COUNT(CASE WHEN quad_class = '2' THEN 1 END) AS quad2_cnt,
        COUNT(CASE WHEN quad_class = '3' THEN 1 END) AS quad3_cnt,
        COUNT(CASE WHEN quad_class = '4' THEN 1 END) AS quad4_cnt,

        -- 데이터 품질(DQ) 지표 계산 (stg_seed_mapping의 원본 코드 컬럼 사용)
        AVG(CASE WHEN regexp_like(action_geo_country_code, '^[A-Z]{2}$') THEN 1.0 ELSE 0.0 END) AS dq_action_geo_alpha_ratio,
        AVG(CASE WHEN regexp_like(action_geo_country_code, '^[0-9.-]+$') THEN 1.0 ELSE 0.0 END) AS dq_action_geo_numeric_ratio,
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