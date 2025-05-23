with daily_metrics as (
    select * from {{ ref('int_daily_metrics') }}
),

-- Calculate 20-day moving averages and other technical indicators
moving_averages as (
    select
        *,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as moving_avg_20d,
        avg(trading_volume) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as volume_avg_20d
    from daily_metrics
)

select
    symbol,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    trading_volume,
    daily_return_pct,
    daily_volatility_pct,
    day_performance,
    moving_avg_20d,
    volume_avg_20d,
    case 
        when close_price > moving_avg_20d then 'ABOVE_MA20'
        else 'BELOW_MA20'
    end as ma20_position,
    case 
        when trading_volume > volume_avg_20d then 'ABOVE_AVG'
        else 'BELOW_AVG'
    end as volume_signal
from moving_averages 