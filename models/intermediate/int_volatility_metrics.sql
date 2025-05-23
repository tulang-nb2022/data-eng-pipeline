with daily_metrics as (
    select * from {{ ref('int_daily_metrics') }}
),

-- Beta calculation (requires market index data)
market_comparison as (
    select
        d.symbol,
        d.trading_date,
        d.daily_return_pct as stock_return,
        m.daily_return_pct as market_return
    from daily_metrics d
    left join {{ ref('stg_alpha_vantage__market_index') }} m  -- Assuming SPY or similar index
        using (trading_date)
),

beta_calc as (
    select
        symbol,
        trading_date,
        covar_pop(stock_return, market_return) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row  -- ~1 year
        ) / var_pop(market_return) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) as beta_1y
    from market_comparison
),

-- Volatility metrics
volatility_calcs as (
    select
        symbol,
        trading_date,
        daily_return_pct,
        stddev(daily_return_pct) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) * sqrt(252) as annual_volatility,  -- Annualized volatility
        stddev(daily_return_pct) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) * sqrt(20) as monthly_volatility
    from daily_metrics
)

select
    v.*,
    b.beta_1y,
    case 
        when annual_volatility > 0.40 then 'HIGH'
        when annual_volatility > 0.20 then 'MEDIUM'
        else 'LOW'
    end as volatility_category
from volatility_calcs v
left join beta_calc b using (symbol, trading_date) 