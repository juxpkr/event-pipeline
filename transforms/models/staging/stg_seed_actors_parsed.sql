-- models/staging/stg_seed_actors_parsed.sql

-- 1. 필요한 모든 Seed 테이블들을 CTE로 미리 정의합니다.
WITH country_iso_codes AS (SELECT * FROM {{ ref('geo_country_iso_codes') }}),
    role_codes AS (SELECT * FROM {{ ref('actor_role_codes') }}),
    organization_codes AS (SELECT * FROM {{ ref('actor_organization_codes') }}),
    ethnic_codes AS (SELECT * FROM {{ ref('actor_ethnic_group_codes') }}),
    religion_codes AS (SELECT * FROM {{ ref('actor_religion_codes') }}),

-- 2. 원본 데이터를 불러와 Actor1과 Actor2 코드를 하나의 목록으로 통합합니다.
source_actors AS (
    SELECT DISTINCT actor1_code AS actor_code
    FROM {{ source('gdelt_silver_layer', 'gdelt_events') }}
    WHERE actor1_code IS NOT NULL

    UNION

    SELECT DISTINCT actor2_code AS actor_code
    FROM {{ source('gdelt_silver_layer', 'gdelt_events') }}
    WHERE actor2_code IS NOT NULL
),

-- 3. Actor 코드를 3글자 단위의 코드 조각(part) 4개로 분해합니다.
actors_decomposed AS (
    SELECT
        actor_code,
        SUBSTRING(actor_code, 1, 3) AS part1,
        CASE WHEN LENGTH(actor_code) >= 6 THEN SUBSTRING(actor_code, 4, 3) ELSE NULL END AS part2,
        CASE WHEN LENGTH(actor_code) >= 9 THEN SUBSTRING(actor_code, 7, 3) ELSE NULL END AS part3,
        CASE WHEN LENGTH(actor_code) >= 12 THEN SUBSTRING(actor_code, 10, 3) ELSE NULL END AS part4
    FROM
        source_actors
),

-- 4. 분해된 코드 조각들을 각 Seed 테이블과 매핑하여 의미를 부여합니다.
actors_mapped AS (
    SELECT
        d.actor_code,

        -- Part 1의 설명(description)과 유형(type) 찾기
        COALESCE(p1_org.description, p1_role.description, p1_country.description, p1_eth.description, p1_rel.description) AS description1,
        CASE
            WHEN p1_org.code IS NOT NULL THEN 'Organization'
            WHEN p1_role.code IS NOT NULL THEN 'Role'
            WHEN p1_country.code IS NOT NULL THEN 'Country'
            WHEN p1_eth.code IS NOT NULL THEN 'Ethnic'
            WHEN p1_rel.code IS NOT NULL THEN 'Religion'
            ELSE NULL
        END AS type1,

        -- Part 2의 설명(description)과 유형(type) 찾기
        COALESCE(p2_org.description, p2_role.description, p2_country.description, p2_eth.description, p2_rel.description) AS description2,
        CASE
            WHEN p2_org.code IS NOT NULL THEN 'Organization'
            WHEN p2_role.code IS NOT NULL THEN 'Role'
            WHEN p2_country.code IS NOT NULL THEN 'Country'
            WHEN p2_eth.code IS NOT NULL THEN 'Ethnic'
            WHEN p2_rel.code IS NOT NULL THEN 'Religion'
            ELSE NULL
        END AS type2,

        -- Part 3의 설명(description)과 유형(type) 찾기
        COALESCE(p3_org.description, p3_role.description, p3_country.description, p3_eth.description, p3_rel.description) AS description3,
        CASE
            WHEN p3_org.code IS NOT NULL THEN 'Organization'
            WHEN p3_role.code IS NOT NULL THEN 'Role'
            WHEN p3_country.code IS NOT NULL THEN 'Country'
            WHEN p3_eth.code IS NOT NULL THEN 'Ethnic'
            WHEN p3_rel.code IS NOT NULL THEN 'Religion'
            ELSE NULL
        END AS type3,
        
        -- Part 4의 설명(description)과 유형(type) 찾기
        COALESCE(p4_org.description, p4_role.description, p4_country.description, p4_eth.description, p4_rel.description) AS description4,
        CASE
            WHEN p4_org.code IS NOT NULL THEN 'Organization'
            WHEN p4_role.code IS NOT NULL THEN 'Role'
            WHEN p4_country.code IS NOT NULL THEN 'Country'
            WHEN p4_eth.code IS NOT NULL THEN 'Ethnic'
            WHEN p4_rel.code IS NOT NULL THEN 'Religion'
            ELSE NULL
        END AS type4

    FROM
        actors_decomposed d
        -- Part 1 Mapping
        LEFT JOIN organization_codes p1_org ON d.part1 = p1_org.code
        LEFT JOIN role_codes p1_role ON d.part1 = p1_role.code
        LEFT JOIN country_iso_codes p1_country ON d.part1 = p1_country.code
        LEFT JOIN ethnic_codes p1_eth ON d.part1 = p1_eth.code
        LEFT JOIN religion_codes p1_rel ON d.part1 = p1_rel.code
        -- Part 2 Mapping
        LEFT JOIN organization_codes p2_org ON d.part2 = p2_org.code
        LEFT JOIN role_codes p2_role ON d.part2 = p2_role.code
        LEFT JOIN country_iso_codes p2_country ON d.part2 = p2_country.code
        LEFT JOIN ethnic_codes p2_eth ON d.part2 = p2_eth.code
        LEFT JOIN religion_codes p2_rel ON d.part2 = p2_rel.code
        -- Part 3 Mapping
        LEFT JOIN organization_codes p3_org ON d.part3 = p3_org.code
        LEFT JOIN role_codes p3_role ON d.part3 = p3_role.code
        LEFT JOIN country_iso_codes p3_country ON d.part3 = p3_country.code
        LEFT JOIN ethnic_codes p3_eth ON d.part3 = p3_eth.code
        LEFT JOIN religion_codes p3_rel ON d.part3 = p3_rel.code
        -- Part 4 Mapping
        LEFT JOIN organization_codes p4_org ON d.part4 = p4_org.code
        LEFT JOIN role_codes p4_role ON d.part4 = p4_role.code
        LEFT JOIN country_iso_codes p4_country ON d.part4 = p4_country.code
        LEFT JOIN ethnic_codes p4_eth ON d.part4 = p4_eth.code
        LEFT JOIN religion_codes p4_rel ON d.part4 = p4_rel.code
)

-- 4단계: 최종 결과물을 SELECT하여 모델을 완성합니다.
SELECT * FROM actors_mapped