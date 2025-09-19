-- 증분 모델 (Incremental Model) 설정
{{ config(
    materialized='incremental',
    unique_key=['global_event_id']
) }}

SELECT
    global_event_id,
    day as event_date,
    actor1_country_code,
    actor1_name,
    event_root_code,
    CAST(avg_tone AS DOUBLE) as avg_tone,
    processed_at,
    1 as event_count
FROM
    {{ source('gdelt_silver_layer', 'gdelt_events')}}
WHERE
    global_event_id IS NOT NULL
    AND day IS NOT NULL

{% if is_incremental() and adapter.get_relation(this.database, this.schema, this.identifier) %}

  -- 이 모델이 이미 데이터를 가지고 있다면,
  -- 최신 날짜보다 더 새로운 데이터만 처리 (3일 버퍼 포함)
  AND processed_at >= date_add((SELECT MAX(processed_at) FROM {{ this }}), -3)

{% endif %}