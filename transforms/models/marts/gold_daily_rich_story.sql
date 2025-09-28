-- [Marts 테이블]: models/marts/gold_daily_rich_story.sql
-- Version: 2.0
-- 역할: BI 대시보드에 표시될 '일일/국가별 대표 스토리' 3종(영향력, 주목도, 이례성)을 선정하는 테이블
-- 실행 주기: 1일 증분

{{ config(
    materialized='incremental',
    unique_key=['event_date', 'mp_action_geo_country_iso', 'story_type']
) }}

-- CTE 1: 새로 들어온 GKG/Mentions 데이터가 속한 날짜들을 먼저 찾습니다. (대표 스토리 재계산을 위함)
WITH new_gkg_events AS (
    SELECT
        global_event_id,
        processed_at,
        event_date
    FROM {{ ref('stg_gkg_detailed_events') }}
    WHERE event_date >= '2025-09-26'
    
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

dates_with_new_events AS (
    SELECT DISTINCT se.event_date
    FROM new_gkg_events AS gkg
    INNER JOIN {{ ref('stg_seed_mapping') }} AS se
        ON gkg.global_event_id = se.global_event_id
    WHERE se.mp_action_geo_country_iso IS NOT NULL
),

-- CTE 2: 위에서 찾은 날짜에 해당하는 모든 이벤트(과거+신규)를 다시 불러와 대표 스토리를 재계산합니다.
events_to_recalculate AS (
    SELECT *
    FROM {{ ref('stg_seed_mapping') }}
    WHERE event_date IN (SELECT event_date FROM dates_with_new_events) AND mp_action_geo_country_iso IS NOT NULL
),

-- CTE 3: Staging 모델들을 모두 조인하여 상세 정보를 만듭니다.
joined_sources AS (
    SELECT
        stg_events.*,
        stg_actor1_info.actor_full_description AS actor1_info,
        stg_actor2_info.actor_full_description AS actor2_info,
        stg_detailed.v2_persons,
        stg_detailed.v2_organizations,
        stg_detailed.v2_enhanced_themes,
        stg_detailed.amounts,
        stg_detailed.mention_source_name
    FROM events_to_recalculate AS stg_events
    LEFT JOIN {{ ref('stg_actors_description') }} AS stg_actor1_info ON stg_events.actor1_code = stg_actor1_info.actor_code
    LEFT JOIN {{ ref('stg_actors_description') }} AS stg_actor2_info ON stg_events.actor2_code = stg_actor2_info.actor_code
    LEFT JOIN {{ ref('stg_gkg_detailed_events') }} AS stg_detailed ON stg_events.global_event_id = stg_detailed.global_event_id
),

-- [성능 최적화] CTE 4: 각 복잡한 필드를 병렬로 처리하고 재집계합니다.
persons_aggregated AS (
    SELECT global_event_id, ARRAY_JOIN(SLICE(COLLECT_SET(INITCAP(TRIM(p))), 1, 2), ', ') AS key_persons
    FROM joined_sources LATERAL VIEW EXPLODE(SPLIT(v2_persons, ';')) exploded_p AS p WHERE v2_persons IS NOT NULL AND LENGTH(TRIM(p)) > 2 GROUP BY 1
),
orgs_aggregated AS (
    SELECT global_event_id, ARRAY_JOIN(SLICE(COLLECT_SET(INITCAP(TRIM(o))), 1, 2), ', ') AS key_organizations
    FROM joined_sources LATERAL VIEW EXPLODE(SPLIT(v2_organizations, ';')) exploded_o AS o WHERE v2_organizations IS NOT NULL AND LENGTH(TRIM(o)) > 3 GROUP BY 1
),
amounts_aggregated AS (
    SELECT global_event_id, ARRAY_JOIN(SLICE(COLLECT_SET(TRIM(a)), 1, 2), ', ') AS key_amounts
    FROM joined_sources LATERAL VIEW EXPLODE(SPLIT(amounts, ';')) exploded_a AS a WHERE amounts IS NOT NULL AND a RLIKE '\\d' GROUP BY 1
),
themes_unpacked AS (
    SELECT global_event_id, TRIM(SPLIT(single_theme, ',')[0]) AS clean_theme_name
    FROM joined_sources LATERAL VIEW EXPLODE(SPLIT(v2_enhanced_themes, ';')) exploded_t AS single_theme
    WHERE v2_enhanced_themes IS NOT NULL AND TRIM(SPLIT(single_theme, ',')[0]) RLIKE '[a-zA-Z]'
),
themes_aggregated AS (
    SELECT global_event_id, ARRAY_JOIN(SLICE(COLLECT_SET(processed_theme), 1, 3), ', ') as rich_themes
    FROM (
        SELECT global_event_id, TRIM(REPLACE(
            CASE -- 모든 theme_mapping 규칙 포함
                WHEN clean_theme_name LIKE 'WB_%' THEN REPLACE(clean_theme_name, 'WB_', '세계은행 주제: ') WHEN clean_theme_name LIKE 'UN_%' THEN REPLACE(clean_theme_name, 'UN_', '유엔 관련: ') WHEN clean_theme_name LIKE 'EU_%' THEN REPLACE(clean_theme_name, 'EU_', '유럽연합 관련: ')
                WHEN clean_theme_name LIKE 'ECON_%' THEN REPLACE(clean_theme_name, 'ECON_', '경제: ') WHEN clean_theme_name LIKE 'EPU_%' THEN REPLACE(clean_theme_name, 'EPU_', '경제정책 불확실성: ') WHEN clean_theme_name LIKE 'TAX_FNCACT%' THEN REPLACE(clean_theme_name, 'TAX_FNCACT_', '세금 및 재정 정책: ')
                WHEN clean_theme_name LIKE 'TAX_%' THEN REPLACE(clean_theme_name, 'TAX_', '세금: ') WHEN clean_theme_name LIKE 'FINANCE%' THEN REPLACE(clean_theme_name, 'FINANCE', '금융/재정') WHEN clean_theme_name LIKE 'FNCACT%' THEN REPLACE(clean_theme_name, 'FNCACT', '금융 활동')
                WHEN clean_theme_name LIKE 'TRADE%' THEN REPLACE(clean_theme_name, 'TRADE', '무역') WHEN clean_theme_name LIKE 'INVEST%' THEN REPLACE(clean_theme_name, 'INVEST', '투자') WHEN clean_theme_name LIKE 'SANCTIONS%' THEN REPLACE(clean_theme_name, 'SANCTIONS', '제재')
                WHEN clean_theme_name LIKE 'UNREST_%' THEN REPLACE(clean_theme_name, 'UNREST_', '사회 불안: ') WHEN clean_theme_name LIKE 'PROTEST%' THEN REPLACE(clean_theme_name, 'PROTEST', '시위') WHEN clean_theme_name LIKE 'STRIKE%' THEN REPLACE(clean_theme_name, 'STRIKE', '파업')
                WHEN clean_theme_name LIKE 'COUP%' THEN REPLACE(clean_theme_name, 'COUP', '쿠데타') WHEN clean_theme_name LIKE 'CONFLICT%' THEN REPLACE(clean_theme_name, 'CONFLICT', '분쟁') WHEN clean_theme_name LIKE 'MILITARY%' THEN REPLACE(clean_theme_name, 'MILITARY', '군사')
                WHEN clean_theme_name LIKE 'SECURITY%' THEN REPLACE(clean_theme_name, 'SECURITY', '안보') WHEN clean_theme_name LIKE 'TERROR%' THEN REPLACE(clean_theme_name, 'TERROR', '테러') WHEN clean_theme_name LIKE 'VIOLENCE%' THEN REPLACE(clean_theme_name, 'VIOLENCE', '폭력')
                WHEN clean_theme_name LIKE 'WAR%' THEN REPLACE(clean_theme_name, 'WAR', '전쟁') WHEN clean_theme_name LIKE 'HUMAN_RIGHTS%' THEN REPLACE(clean_theme_name, 'HUMAN_RIGHTS', '인권') WHEN clean_theme_name LIKE 'GENOCIDE%' THEN REPLACE(clean_theme_name, 'GENOCIDE', '집단학살')
                WHEN clean_theme_name LIKE 'JUSTICE%' THEN REPLACE(clean_theme_name, 'JUSTICE', '사법') WHEN clean_theme_name LIKE 'LAW%' THEN REPLACE(clean_theme_name, 'LAW', '법률') WHEN clean_theme_name LIKE 'SOVEREIGNTY%' THEN REPLACE(clean_theme_name, 'SOVEREIGNTY', '주권')
                WHEN clean_theme_name LIKE 'CRISISLEX_%' THEN REPLACE(clean_theme_name, 'CRISISLEX_', '위기: ') WHEN clean_theme_name LIKE 'NATURAL_DISASTER_%' THEN REPLACE(clean_theme_name, 'NATURAL_DISASTER_', '자연재해: ')
                WHEN clean_theme_name LIKE 'DISASTER%' THEN REPLACE(clean_theme_name, 'DISASTER', '재해') WHEN clean_theme_name LIKE 'FLOOD%' THEN REPLACE(clean_theme_name, 'FLOOD', '홍수') WHEN clean_theme_name LIKE 'EARTHQUAKE%' THEN REPLACE(clean_theme_name, 'EARTHQUAKE', '지진')
                WHEN clean_theme_name LIKE 'FIRE%' THEN REPLACE(clean_theme_name, 'FIRE', '화재') WHEN clean_theme_name LIKE 'EPIDEMIC%' THEN REPLACE(clean_theme_name, 'EPIDEMIC', '전염병') WHEN clean_theme_name LIKE 'AID_%' THEN REPLACE(clean_theme_name, 'AID_', '원조: ')
                WHEN clean_theme_name LIKE 'FOOD_SECURITY%' THEN REPLACE(clean_theme_name, 'FOOD_SECURITY', '식량 안보') WHEN clean_theme_name LIKE 'REFUGEE%' THEN REPLACE(clean_theme_name, 'REFUGEE', '난민') WHEN clean_theme_name LIKE 'HUMANITARIAN%' THEN REPLACE(clean_theme_name, 'HUMANITARIAN', '인도주의')
                WHEN clean_theme_name LIKE 'CLIMATE%' THEN REPLACE(clean_theme_name, 'CLIMATE', '기후') WHEN clean_theme_name LIKE 'ENERGY%' THEN REPLACE(clean_theme_name, 'ENERGY', '에너지') WHEN clean_theme_name LIKE 'OIL%' THEN REPLACE(clean_theme_name, 'OIL', '석유/가스')
                WHEN clean_theme_name LIKE 'WATER%' THEN REPLACE(clean_theme_name, 'WATER', '물 자원') WHEN clean_theme_name LIKE 'FOREST%' THEN REPLACE(clean_theme_name, 'FOREST', '산림') WHEN clean_theme_name LIKE 'MEDIA%' THEN REPLACE(clean_theme_name, 'MEDIA', '미디어')
                WHEN clean_theme_name LIKE 'INTERNET%' THEN REPLACE(clean_theme_name, 'INTERNET', '인터넷') WHEN clean_theme_name LIKE 'ICT%' THEN REPLACE(clean_theme_name, 'ICT', '정보통신기술') WHEN clean_theme_name LIKE 'SOCIAL_MEDIA%' THEN REPLACE(clean_theme_name, 'SOCIAL_MEDIA', '소셜 미디어')
                WHEN clean_theme_name LIKE 'ELECTION%' THEN REPLACE(clean_theme_name, 'ELECTION', '선거') WHEN clean_theme_name LIKE 'CORRUPTION%' THEN REPLACE(clean_theme_name, 'CORRUPTION', '부패') WHEN clean_theme_name LIKE 'TRANSPARENCY%' THEN REPLACE(clean_theme_name, 'TRANSPARENCY', '투명성')
                WHEN clean_theme_name LIKE 'CRIME%' THEN REPLACE(clean_theme_name, 'CRIME', '범죄') WHEN clean_theme_name LIKE 'DRUG%' THEN REPLACE(clean_theme_name, 'DRUG', '마약') WHEN clean_theme_name LIKE 'ORGANIZED_CRIME%' THEN REPLACE(clean_theme_name, 'ORGANIZED_CRIME', '조직범죄')
                ELSE clean_theme_name
            END, '_', ' ')) as processed_theme
        FROM themes_unpacked
    ) GROUP BY 1
),

-- CTE 5: 모든 처리된 데이터를 최종 조인하고, Z-Score와 rich_story를 생성합니다.
final_join AS (
    SELECT
        j.*,
        p.key_persons,
        o.key_organizations,
        a.key_amounts,
        t.rich_themes
    FROM joined_sources j
    LEFT JOIN persons_aggregated p ON j.global_event_id = p.global_event_id
    LEFT JOIN orgs_aggregated o ON j.global_event_id = o.global_event_id
    LEFT JOIN amounts_aggregated a ON j.global_event_id = a.global_event_id
    LEFT JOIN themes_aggregated t ON j.global_event_id = t.global_event_id
),
stories_with_kpi AS (
    SELECT
        *,
        (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) AS goldstein_zscore,
        (avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0) AS tone_zscore,
        CASE WHEN (goldstein_scale - AVG(goldstein_scale) OVER()) / NULLIF(STDDEV(goldstein_scale) OVER(), 0) > 2 OR ABS((avg_tone - AVG(avg_tone) OVER()) / NULLIF(STDDEV(avg_tone) OVER(), 0)) > 2 THEN true ELSE false END AS is_anomaly,
        CASE WHEN quad_class IN (1, 2) THEN '협력' WHEN quad_class IN (3, 4) THEN '갈등' ELSE '중립' END AS event_type,
        CASE
            WHEN v2_persons IS NULL AND v2_organizations IS NULL AND rich_themes IS NULL THEN
                COALESCE(mp_action_geo_country_kor, '알 수 없는 국가') || '에서 ' || COALESCE(actor1_info, actor1_name, '알 수 없는 주체') || '와(과) ' || COALESCE(actor2_info, actor2_name, '알 수 없는 대상') || 
                ' 간의 ' || CASE WHEN avg_tone > 0 THEN '긍정적인 ' ELSE '부정적인 ' END || COALESCE(mp_event_categories, '알 수 없는') || 
                ' 이벤트가 발생했습니다.'
            ELSE
                -- 1. 중요도 접두사
                COALESCE(CASE WHEN ABS(goldstein_scale) > 8 THEN '주요 사건: ' WHEN ABS(goldstein_scale) > 5 THEN '중요 사건: ' WHEN ABS(goldstein_scale) > 3 THEN '주목할 만한 사건: ' ELSE '' END, '') ||
                -- 2. 메인 스토리 문장
                COALESCE(actor1_info, actor1_name, '알 수 없는 주체') || '이(가) ' ||
                COALESCE(mp_action_geo_country_kor, '알 수 없는 국가') || '에서 ' ||
                CASE WHEN actor2_name IS NOT NULL THEN COALESCE(actor2_info, actor2_name, '알 수 없는 대상') || '에게 ' ELSE '' END ||
                CASE WHEN avg_tone > 5 THEN '매우 긍정적인 방식으로 ' WHEN avg_tone > 2 THEN '긍정적인 감정으로 ' WHEN avg_tone < -5 THEN '강한 부정적 감정으로 ' WHEN avg_tone < -2 THEN '긴장 상황 속에서 ' ELSE '중립적인 톤으로 ' END ||
                COALESCE(mp_event_info, '알 수 없는 행동') || ' 관련 논의를 했습니다.' ||
                -- 3. 보조 정보 (괄호 안)
                COALESCE(' (테마: ' || rich_themes || ')', '') ||
                COALESCE(' (핵심 인물: ' || key_persons || ')', '') ||
                COALESCE(' (핵심 단체: ' || key_organizations || ')', '') ||
                COALESCE(' (규모: ' || key_amounts || ')', '') ||
                ' (' || COALESCE(num_articles, 0) || '개 기사에서 보도됨' || CASE WHEN mention_source_name IS NOT NULL THEN ', ' || mention_source_name || ' 포함' ELSE '' END || ')'
        END AS rich_story
    FROM final_join
),

-- CTE 6: 3가지 기준별로 순위를 매깁니다.
ranked_by_impact AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_date, mp_action_geo_country_iso ORDER BY goldstein_scale ASC, processed_at DESC) as rnk
    FROM stories_with_kpi
),
ranked_by_attention AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_date, mp_action_geo_country_iso ORDER BY num_articles DESC, processed_at DESC) as rnk
    FROM stories_with_kpi
),
ranked_by_anomaly AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_date, mp_action_geo_country_iso ORDER BY GREATEST(ABS(goldstein_zscore), ABS(tone_zscore)) DESC, processed_at DESC) as rnk
    FROM stories_with_kpi
    WHERE is_anomaly = true
),

-- [신규] CTE 7: 이례적 이벤트가 없는 날짜/국가 쌍을 찾기 위한 준비
all_country_day_pairs AS (
    SELECT DISTINCT
        event_date,
        mp_action_geo_country_iso,
        mp_action_geo_country_eng,
        mp_action_geo_country_kor
    FROM stories_with_kpi
),
anomaly_stories_with_placeholders AS (
    SELECT
        base.event_date,
        base.mp_action_geo_country_iso,
        base.mp_action_geo_country_eng,
        base.mp_action_geo_country_kor,
        '가장 이례적인 이벤트' AS story_type,
        COALESCE(anomaly.rich_story, '해당 날짜에 이례적인 이벤트가 발생하지 않았습니다.') AS rich_story,
        anomaly.goldstein_scale,
        anomaly.avg_tone,
        anomaly.event_type,
        anomaly.source_url,
        anomaly.processed_at
    FROM all_country_day_pairs AS base
    LEFT JOIN ranked_by_anomaly AS anomaly
        ON base.event_date = anomaly.event_date
        AND base.mp_action_geo_country_iso = anomaly.mp_action_geo_country_iso
        AND anomaly.rnk = 1
)

-- 최종 SELECT: 각 기준별 1위 스토리를 UNION으로 합칩니다.
SELECT
    event_date, mp_action_geo_country_iso, mp_action_geo_country_eng, mp_action_geo_country_kor,
    '가장 영향력 있는 이벤트' AS story_type, rich_story, goldstein_scale, avg_tone, event_type, source_url, processed_at
FROM ranked_by_impact WHERE rnk = 1

UNION ALL

SELECT
    event_date, mp_action_geo_country_iso, mp_action_geo_country_eng, mp_action_geo_country_kor,
    '가장 주목받은 이벤트' AS story_type, rich_story, goldstein_scale, avg_tone, event_type, source_url, processed_at
FROM ranked_by_attention WHERE rnk = 1

UNION ALL

SELECT
    event_date, mp_action_geo_country_iso, mp_action_geo_country_eng, mp_action_geo_country_kor,
    story_type, rich_story, goldstein_scale, avg_tone, event_type, source_url, processed_at
FROM anomaly_stories_with_placeholders