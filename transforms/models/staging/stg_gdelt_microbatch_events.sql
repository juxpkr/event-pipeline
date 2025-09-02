SELECT
    global_event_id,
    day as event_date,
    actor1_country_code,
    actor1_name,
    event_root_code,
    CAST(avg_tone AS DOUBLE) as avg_tone,
    processed_time,
    1 as event_count
FROM
    {{ source('gdelt_silver_layer', 'gdelt_silver_events')}}
WHERE 
    global_event_id IS NOT NULL
    AND day IS NOT NULL