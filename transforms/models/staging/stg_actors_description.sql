-- [Staging 테이블]: models/staging/stg_actors_description.sql
-- Version : 2.0
-- Actor 코드를 분해, 매핑, 조합하여 최종 Actor 설명문을 생성
-- 기존 stg_seed_actors_parsed와 stg_actors_description 모델 통합 -> 최적화
-- 가변 길이 Actor 코드를 처리하기 위해 직접 매칭(Direct Match) 로직 추가
-- View로 작동하므로, 증분 관련 코드를 제거함

{{ config(materialized='view') }}

-- 1. CTE: 필요한 모든 Seed 테이블을 하나의 '통합 코드 사전'으로 합치고 우선순위를 부여합니다.
WITH all_codes_with_type AS (
    SELECT code, description, '조직' AS type, 5 as priority FROM {{ ref('actor_organization_codes') }}
    UNION ALL
    SELECT code, description, '역할' AS type, 4 as priority FROM {{ ref('actor_role_codes') }}
    UNION ALL
    SELECT iso_code AS code, name_kor AS description, '국가' AS type, 1 as priority FROM {{ ref('geo_country_codes') }}
    UNION ALL
    SELECT code, description, '민족' AS type, 2 as priority FROM {{ ref('actor_ethnic_group_codes') }}
    UNION ALL
    SELECT code, description, '종교' AS type, 3 as priority FROM {{ ref('actor_religion_codes') }}
),

-- 2. CTE: 원본 데이터에서 중복 없는 Actor 코드 목록을 추출합니다.
source_actors AS (
    SELECT DISTINCT actor_code FROM (
        SELECT actor1_code AS actor_code FROM {{ source('gdelt_silver_layer', 'gdelt_events') }} WHERE actor1_code IS NOT NULL AND event_date >= '2023-09-01'
        UNION ALL
        SELECT actor2_code AS actor_code FROM {{ source('gdelt_silver_layer', 'gdelt_events') }} WHERE actor2_code IS NOT NULL AND event_date >= '2023-09-01'
    )
),

-- 3. CTE: actor_code 전체가 코드 사전에 있는지 직접 매칭을 시도합니다.
direct_match AS (
    SELECT
        actor_code,
        description AS actor_full_description
    FROM source_actors
    JOIN all_codes_with_type ON source_actors.actor_code = all_codes_with_type.code
),

-- 4. CTE: 직접 매칭에 실패한 코드들만 분해 대상으로 삼습니다.
actors_to_decompose AS (
    SELECT actor_code
    FROM source_actors
    EXCEPT
    SELECT actor_code
    FROM direct_match
),

-- 5. CTE: 분해 대상 코드를 3글자 단위로 분해합니다.
actors_decomposed AS (
    SELECT
        actor_code,
        SUBSTRING(actor_code, 1, 3) AS part1,
        CASE WHEN LENGTH(actor_code) >= 6 THEN SUBSTRING(actor_code, 4, 3) ELSE NULL END AS part2,
        CASE WHEN LENGTH(actor_code) >= 9 THEN SUBSTRING(actor_code, 7, 3) ELSE NULL END AS part3,
        CASE WHEN LENGTH(actor_code) >= 12 THEN SUBSTRING(actor_code, 10, 3) ELSE NULL END AS part4
    FROM actors_to_decompose
),

-- 6. CTE: 분해된 코드 조각들을 구조체(Struct)로 만들고 우선순위를 부여합니다.
parts_mapped AS (
    SELECT
        d.actor_code,
        STRUCT(p1.priority, 1 AS part_order, p1.description AS description) AS part1_struct,
        STRUCT(p2.priority, 2 AS part_order, p2.description AS description) AS part2_struct,
        STRUCT(p3.priority, 3 AS part_order, p3.description AS description) AS part3_struct,
        STRUCT(p4.priority, 4 AS part_order, p4.description AS description) AS part4_struct
    FROM actors_decomposed d
    LEFT JOIN all_codes_with_type p1 ON d.part1 = p1.code
    LEFT JOIN all_codes_with_type p2 ON d.part2 = p2.code
    LEFT JOIN all_codes_with_type p3 ON d.part3 = p3.code
    LEFT JOIN all_codes_with_type p4 ON d.part4 = p4.code
),

-- 7. CTE: 분해된 코드들의 설명문을 조합합니다. (Spark 순서 보장 로직 포함)
decomposed_descriptions AS (
    SELECT
        actor_code,
        -- ARRAY_REMOVE 대신 FILTER를 사용하여 description이 NULL이 아닌 Struct만 확실하게 걸러냅니다.
        ARRAY_JOIN(
            TRANSFORM(
                SORT_ARRAY(
                    FILTER(
                        ARRAY(part1_struct, part2_struct, part3_struct, part4_struct),
                        x -> x.description IS NOT NULL
                    )
                ),
                p -> p.description
            ),
            ' '
        ) AS actor_full_description
    FROM parts_mapped
)

-- 8. 최종 SELECT: 직접 매칭된 결과와, 분해 후 조합된 결과를 합칩니다.
SELECT actor_code, actor_full_description FROM direct_match
UNION ALL
SELECT actor_code, actor_full_description FROM decomposed_descriptions