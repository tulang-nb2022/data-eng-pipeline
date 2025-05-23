with daily_prices as (
    select * from {{ ref('stg_alpha_vantage__daily_prices') }}
),

price_changes as (
    select
        symbol,
        trading_date,
        close_price,
        open_price,
        high_price,
        low_price,
        trading_volume,
        lag(close_price) over (partition by symbol order by trading_date) as previous_close,
        lead(close_price) over (partition by symbol order by trading_date) as next_close
    from daily_prices
),

metrics as (
    select
        *,
        ((close_price - previous_close) / previous_close) * 100 as daily_return_pct,
        ((high_price - low_price) / open_price) * 100 as daily_volatility_pct,
        case 
            when close_price > open_price then 'GAIN'
            when close_price < open_price then 'LOSS'
            else 'NEUTRAL'
        end as day_performance
    from price_changes
)

select * from metrics 