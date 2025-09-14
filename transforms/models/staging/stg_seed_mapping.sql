-- models/staging/stg_seed_mapping.sql

-- 1. 필요한 테이블들을 CTE(WITH 절)로 미리 정의합니다.
WITH source_data AS (SELECT * FROM {{ source('gdelt_silver_layer', 'gdelt_silver_events') }}),
    event_root_codes AS (SELECT * FROM {{ ref('event_root_codes') }}),
    event_detail_codes AS (SELECT * FROM {{ ref('event_detail_codes') }}),
    quad_class_codes AS (SELECT * FROM {{ ref('event_quad_class_codes') }}),
    country_iso_codes AS (SELECT * FROM {{ ref('geo_country_iso_codes') }}),
    country_fips_codes AS (SELECT * FROM {{ ref('geo_country_fips_codes') }}),
    adm_codes AS (SELECT * FROM {{ ref('geo_adm_codes') }}),
    role_codes AS (SELECT * FROM {{ ref('actor_role_codes') }}),
    organization_codes AS (SELECT * FROM {{ ref('actor_organization_codes') }}),
    ethnic_codes AS (SELECT * FROM {{ ref('actor_ethnic_group_codes') }}),
    religion_codes AS (SELECT * FROM {{ ref('actor_religion_codes') }}),
    geo_type_codes AS (SELECT * FROM {{ ref('geo_type_codes') }})

-- 2. CTE들을 JOIN하여 코드들을 실제 설명으로 변환(매핑)합니다.
SELECT
    -- 이벤트 기본 정보
    src.global_event_id,
    src.event_date,

    -- 이벤트 세부 정보
    -- src.is_root_event,
    CASE 
        WHEN src.is_root_event = 1 THEN true
        ELSE false 
    END AS is_root_event,   -- Boolean 변환
    -- src.event_code,
    -- src.event_base_code,
    -- src.event_root_code,
    evtr.description AS mp_event_categories,   -- 루트 설명은 별도 컬럼으로도 제공
    COALESCE(evtd.description, evtr.description) AS mp_event_info,   -- 이벤트 상세 설명
    src.quad_class,
    quad.description AS mp_quad_class,
    src.goldstein_scale,
    src.num_mentions,
    src.num_sources,
    src.num_articles,
    src.avg_tone,

    -- 행위자1(Actor1) 정보(지리 포함) 매핑
    src.actor1_code,
    src.actor1_name,
    src.actor1_country_code,
    a1_iso.description AS mp_actor1_affiliation_country, -- 행위자1 '소속' 국가
    -- src.actor1_known_group_code,
    a1_org.description AS mp_actor1_organization,
    a1_org.type AS mp_actor1_organization_type,
    -- src.actor1_ethnic_code,
    a1_eth.description AS mp_actor1_ethnic,
    -- src.actor1_religion1_code,
    -- src.actor1_religion2_code,
    a1_rel.description AS mp_actor1_religion,
    a1_rel.type AS mp_actor1_religion_type,
    -- src.actor1_type1_code,
    -- src.actor1_type2_code,
    -- src.actor1_type3_code,
    a1_role.description AS mp_actor1_role,
    a1_role.type AS mp_actor1_role_type,
    -- src.actor1_geo_type,
    a1_geo.description AS mp_actor1_geo_type,
    -- src.actor1_geo_fullname,
    -- src.actor1_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.actor1_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.actor1_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.actor1_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: actor1_geo_country_code(FIPS)를 매핑한 결과
        a1_fips.description
    ) AS mp_actor1_location_country,   -- 행위자1 '위치' 국가
    src.actor1_geo_adm1_code,
    src.actor1_geo_lat,
    src.actor1_geo_long,
    src.actor1_geo_feature_id,
 
    -- 행위자2(Actor2) 정보(지리 포함) 매핑
    src.actor2_code,
    src.actor2_name,
    -- src.actor2_country_code,
    a2_iso.description AS mp_actor2_affiliation_country, -- 행위자2 '소속' 국가
    -- src.actor2_known_group_code,
    a2_org.description AS mp_actor2_organization,
    a2_org.type AS mp_actor2_organization_type,
    -- src.actor2_ethnic_code,
    a2_eth.description AS mp_actor2_ethnic,
    -- src.actor2_religion1_code,
    -- src.actor2_religion2_code,
    a2_rel.description AS mp_actor2_religion,
    a2_rel.type AS mp_actor2_religion_type,
    -- src.actor2_type1_code,
    -- src.actor2_type2_code,
    -- src.actor2_type3_code,
    a2_role.description AS mp_actor2_role,
    a2_role.type AS mp_actor2_role_type,
    -- src.actor2_geo_type,
    a2_geo.description AS mp_actor2_geo_type,
    -- src.actor2_geo_fullname,
    -- src.actor2_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.actor2_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.actor2_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.actor2_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: actor2_geo_country_code(FIPS)를 매핑한 결과
        a2_fips.description
    ) AS mp_actor2_location_country,   -- 행위자2 '위치' 국가
    src.actor2_geo_adm1_code,
    src.actor2_geo_lat,
    src.actor2_geo_long,
    src.actor2_geo_feature_id,

    -- 이벤트 지리(Action_geo) 정보 매핑
    -- src.action_geo_type,
    ac_geo.description AS mp_action_geo_type,
    -- src.action_geo_fullname,
    src.action_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.action_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.action_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.action_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: action_geo_country_code(FIPS)를 매핑한 결과
        ac_fips.description
    ) AS mp_action_location_country,
    src.action_geo_adm1_code,
    src.action_geo_lat,
    src.action_geo_long,
    src.action_geo_feature_id,

    -- 데이터 관리용 정보
    src.date_added,
    src.source_url,
    -- src.actor1_geo_centroid,
    -- src.actor2_geo_centroid,
    -- src.action_geo_centroid,
    src.processed_time,
    src.source_file

FROM
    source_data AS src

-- 각 코드 필드를 해당하는 seed 테이블과 LEFT JOIN 합니다.
LEFT JOIN event_root_codes AS evtr ON src.event_root_code = evtr.code
LEFT JOIN event_detail_codes AS evtd ON src.event_code = evtd.code
LEFT JOIN quad_class_codes AS quad ON src.quad_class = quad.code
LEFT JOIN country_iso_codes AS a1_iso ON src.actor1_country_code = a1_iso.code
LEFT JOIN country_iso_codes AS a2_iso ON src.actor2_country_code = a2_iso.code
LEFT JOIN country_fips_codes AS a1_fips ON src.actor1_geo_country_code = a1_fips.code
LEFT JOIN country_fips_codes AS a2_fips ON src.actor2_geo_country_code = a2_fips.code
LEFT JOIN country_fips_codes AS ac_fips ON src.action_geo_country_code = ac_fips.code
LEFT JOIN adm_codes AS a1_adm ON src.actor1_geo_adm1_code = a1_adm.code
LEFT JOIN adm_codes AS a2_adm ON src.actor2_geo_adm1_code = a2_adm.code
LEFT JOIN adm_codes AS ac_adm ON src.action_geo_adm1_code = ac_adm.code
LEFT JOIN geo_type_codes AS a1_geo ON src.actor1_geo_type = a1_geo.code
LEFT JOIN geo_type_codes AS a2_geo ON src.actor2_geo_type = a2_geo.code
LEFT JOIN geo_type_codes AS ac_geo ON src.action_geo_type = ac_geo.code
LEFT JOIN role_codes AS a1_role ON src.actor1_type1_code = a1_role.code
LEFT JOIN role_codes AS a2_role ON src.actor2_type1_code = a2_role.code
LEFT JOIN organization_codes AS a1_org ON src.actor1_known_group_code = a1_org.code
LEFT JOIN organization_codes AS a2_org ON src.actor2_known_group_code = a2_org.code
LEFT JOIN ethnic_codes AS a1_eth ON src.actor1_ethnic_code = a1_eth.code
LEFT JOIN ethnic_codes AS a2_eth ON src.actor2_ethnic_code = a2_eth.code
LEFT JOIN religion_codes AS a1_rel ON src.actor1_religion1_code = a1_rel.code
LEFT JOIN religion_codes AS a2_rel ON src.actor2_religion1_code = a2_rel.code