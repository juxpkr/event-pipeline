with source_data as (
    SELECT * FROM {{ ref('stg_gdelt_microbatch_events') }}
)

SELECT
    event_date,
    actor1_country_code,
    event_root_code,
    COUNT(*) as total_events,
    AVG(avg_tone) as average_tone,
    MIN(avg_tone) as min_tone,
    MAX(avg_tone) as max_tone,
    COUNT(DISTINCT actor1_name) as unique_actors,
    MAX(processed_at) as last_processed_time
FROM source_data
WHERE
    global_event_id IS NOT NULL
    AND actor1_country_code IS NOT NULL
GROUP BY
    event_date,
    actor1_country_code,
    event_root_code