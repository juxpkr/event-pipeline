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
    src.is_root_event,
    src.event_code,
    src.event_base_code,
    src.event_root_code,
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
    src.actor1_geo_fullname,
    src.actor1_country_code,
    src.actor1_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.actor1_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.actor1_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.actor1_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: actor1_country_code(ISO)를 매핑한 결과
        iso1.description,
        -- 3순위: actor1_geo_country_code(FIPS)를 매핑한 결과
        fips1.description
    ) AS mp_actor1_country,
    src.actor1_known_group_code,
    org1.description AS mp_actor1_organization,
    org1.type AS mp_actor1_organization_type,
    src.actor1_ethnic_code,
    eth1.description AS mp_actor1_ethnic,
    src.actor1_religion1_code,
    src.actor1_religion2_code,
    rel1.description AS mp_actor1_religion,
    rel1.type AS mp_actor1_religion_type,
    src.actor1_type1_code,
    src.actor1_type2_code,
    src.actor1_type3_code,
    role1.description AS mp_actor1_role,
    role1.type AS mp_actor1_role_type,
    src.actor1_geo_type,
    src.actor1_geo_adm1_code,
    src.actor1_geo_lat,
    src.actor1_geo_long,
    src.actor1_geo_feature_id,
 
    -- 행위자2(Actor2) 정보(지리 포함) 매핑
    src.actor2_code,
    src.actor2_name,
    src.actor2_geo_fullname,
    src.actor2_country_code,
    src.actor2_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.actor2_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.actor2_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.actor2_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: actor2_country_code(ISO)를 매핑한 결과
        iso2.description,
        -- 3순위: actor2_geo_country_code(FIPS)를 매핑한 결과
        fips2.description
    ) AS mp_actor2_country,
    src.actor2_known_group_code,
    org2.description AS mp_actor2_organization,
    org2.type AS mp_actor2_organization_type,
    src.actor2_ethnic_code,
    eth2.description AS mp_actor2_ethnic,
    src.actor2_religion1_code,
    src.actor2_religion2_code,
    rel2.description AS mp_actor2_religion,
    rel2.type AS mp_actor2_religion_type,
    src.actor2_type1_code,
    src.actor2_type2_code,
    src.actor2_type3_code,
    role2.description AS mp_actor2_role,
    role2.type AS mp_actor2_role_type,
    src.actor2_geo_type,
    src.actor2_geo_adm1_code,
    src.actor2_geo_lat,
    src.actor2_geo_long,
    src.actor2_geo_feature_id,

    -- 이벤트 지리(Action_geo) 정보 매핑
    src.action_geo_type,
    src.action_geo_fullname,
    src.action_geo_country_code,
    COALESCE(
        -- 1순위: fullname에 국가명이 포함된 경우, 우선 사용
        CASE
            WHEN POSITION(',' IN src.action_geo_fullname) > 0 AND LENGTH(TRIM(element_at(split(src.action_geo_fullname, ','), -1))) > 3
            THEN TRIM(element_at(split(src.action_geo_fullname, ','), -1))
            ELSE NULL
        END,
        -- 2순위: action_geo_country_code(FIPS)를 매핑한 결과
        fips_country.description
    ) AS mp_action_location_name,
    geo_type.description AS mp_action_geo_type_description,
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
LEFT JOIN country_iso_codes AS iso1 ON src.actor1_country_code = iso1.code
LEFT JOIN country_iso_codes AS iso2 ON src.actor2_country_code = iso2.code
LEFT JOIN country_fips_codes AS fips1 ON src.actor1_geo_country_code = fips1.code
LEFT JOIN country_fips_codes AS fips2 ON src.actor2_geo_country_code = fips2.code
LEFT JOIN country_fips_codes AS fips_country ON src.action_geo_country_code = fips_country.code
LEFT JOIN adm_codes AS fips_adm1 ON src.action_geo_adm1_code = fips_adm1.code
LEFT JOIN role_codes AS role1 ON src.actor1_type1_code = role1.code
LEFT JOIN role_codes AS role2 ON src.actor2_type1_code = role2.code
LEFT JOIN organization_codes AS org1 ON src.actor1_known_group_code = org1.code
LEFT JOIN organization_codes AS org2 ON src.actor2_known_group_code = org2.code
LEFT JOIN ethnic_codes AS eth1 ON src.actor1_ethnic_code = eth1.code
LEFT JOIN ethnic_codes AS eth2 ON src.actor2_ethnic_code = eth2.code
LEFT JOIN religion_codes AS rel1 ON src.actor1_religion1_code = rel1.code
LEFT JOIN religion_codes AS rel2 ON src.actor2_religion1_code = rel2.code
LEFT JOIN geo_type_codes AS geo_type ON src.action_geo_type = geo_type.code