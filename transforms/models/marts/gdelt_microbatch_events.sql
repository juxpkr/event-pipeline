with source_data as (
    SELECT * FROM {{ ref('stg_gdelt_microbatch') }}
)

SELECT
    CAST(event_date AS DATE) as event_date,
    actor1_country_code,
    event_root_code,
    COUNT(*) as total_events_15min,
    AVG(avg_tone) as average_tone,
    MIN(avg_tone) as min_tone,
    MAX(avg_tone) as max_tone,
    COUNT(DISTINCT actor1_name) as unique_actors
FROM source_data
WHERE
    event_date = CURRENT_DATE() -- 오늘 데이터만 (15분 배치용)
GROUP BY
    CAST(event_date AS DATE),
    actor1_country_code,
    event_root_code