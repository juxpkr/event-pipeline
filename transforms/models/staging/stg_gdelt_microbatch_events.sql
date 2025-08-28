{{
  config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold/'
  )
}}

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
    delta.`s3a://silver/gdelt_events`
WHERE 
    global_event_id IS NOT NULL
    AND day IS NOT NULL