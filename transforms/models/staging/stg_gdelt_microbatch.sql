SELECT
    GlobalEventID as global_event_id,
    Day as event_date,
    Actor1CountryCode as actor1_country_code,
    Actor1Name as actor1_name,
    EventRootCode as event_root_code,
    CAST(AvgTone AS DOUBLE) as avg_tone,
    EXTRACT(DOW FROM CAST(Day AS DATE)) as day_of_week,
    1 as event_count
FROM
    {{ source('gdelt_silver_layer', 'gdelt_events') }}
WHERE 
    GlobalEventID IS NOT NULL
    AND Day IS NOT NULL