{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'trading_date']
    )
}}

with trading_data as (
    select * from {{ ref('fct_daily_trading') }}
    {% if is_incremental() %}
    where trading_date >= (select max(trading_date) from {{ this }})
    {% endif %}
),

summary as (
    select
        symbol,
        trading_date,
        close_price,
        daily_return_pct,
        daily_volatility_pct,
        trading_volume,
        ma20_position,
        volume_signal,
        row_number() over (partition by symbol order by trading_date desc) as recency_rank
    from trading_data
)

select * from summary
where recency_rank = 1  -- Get most recent data for each symbol 