-- models/staging/mart_actor_info_test.sql

WITH staging_events AS (
    -- 모든 코드가 1차 매핑된 staging 모델
    SELECT * FROM {{ ref('stg_seed_mapping') }}
),

parsed_actors AS (
    -- Actor 코드를 전문적으로 분해한 staging 모델
    SELECT * FROM {{ ref('stg_seed_actors_parsed') }}
),

-- 1. 4개의 파트를 세로 목록으로 분해하고, 우선순위를 부여합니다.
unpivoted_actors AS (
    SELECT actor_code, description1 AS description, type1 AS type, 1 AS part_order FROM parsed_actors WHERE description1 IS NOT NULL AND type1 IS NOT NULL
    UNION ALL
    SELECT actor_code, description2, type2, 2 FROM parsed_actors WHERE description2 IS NOT NULL AND type2 IS NOT NULL
    UNION ALL
    SELECT actor_code, description3, type3, 3 FROM parsed_actors WHERE description3 IS NOT NULL AND type3 IS NOT NULL
    UNION ALL
    SELECT actor_code, description4, type4, 4 FROM parsed_actors WHERE description4 IS NOT NULL AND type4 IS NOT NULL
),

-- 2. 분해된 목록을 우선순위에 따라 정렬합니다.
ordered_actors AS (
    SELECT
        actor_code,
        description
    FROM unpivoted_actors
    ORDER BY
        actor_code,
        CASE 
            WHEN type = 'Country' THEN 1
            WHEN type = 'Ethnic' THEN 2
            WHEN type = 'Religion' THEN 3
            WHEN type = 'Role' THEN 4
            WHEN type = 'Organization' THEN 5
            ELSE 99
        END,
        part_order -- 같은 우선 순위일 경우, 원래 코드 순서를 따릅니다.
),

-- 3. 정렬된 설명들을 actor_code별로 하나의 문장으로 합칩니다.
final_actor_descriptions AS (
    SELECT
        actor_code,
        TRIM(ARRAY_JOIN(COLLECT_LIST(description), ' ')) AS actor_full_description
    FROM
        ordered_actors
    GROUP BY
        actor_code
)

SELECT
    stg_evt.global_event_id,

    -- 최종적으로 결합된 Actor1과 Actor2의 설명을 가져옵니다.
    actor1_desc.actor_full_description AS actor1_info,
    actor2_desc.actor_full_description AS actor2_info

FROM
    staging_events stg_evt

LEFT JOIN final_actor_descriptions AS actor1_desc ON stg_evt.actor1_code = actor1_desc.actor_code
LEFT JOIN final_actor_descriptions AS actor2_desc ON stg_evt.actor2_code = actor2_desc.actor_code