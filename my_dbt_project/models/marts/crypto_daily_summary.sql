{{ config(materialized='table') }}

with source_data as(
    select * from {{ source('public', 'stg_crypto_data') }}
)

select
    market_code,
    max(trade_price) as daily_high_price,
    min(trade_price) as daily_low_price,
    avg(trade_price) as daily_avg_price,
    max(volume_24h) as daily_volume
from source_data
group by
    market_code