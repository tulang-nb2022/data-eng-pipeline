with daily_trading as (
    select * from {{ ref('fct_daily_trading') }}
),

technical_indicators as (
    select * from {{ ref('int_technical_indicators') }}
),

volatility_metrics as (
    select * from {{ ref('int_volatility_metrics') }}
),

-- Performance metrics
performance_calcs as (
    select
        symbol,
        trading_date,
        close_price,
        daily_return_pct,
        sum(daily_return_pct) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) as cumulative_yearly_return,
        avg(daily_return_pct) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) * 252 as annualized_return,
        min(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) as fifty_two_week_low,
        max(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 251 preceding and current row
        ) as fifty_two_week_high
    from daily_trading
)

select
    p.*,
    t.rsi_14d,
    t.macd_line,
    t.signal_line,
    t.macd_histogram,
    t.bollinger_signal,
    v.annual_volatility,
    v.monthly_volatility,
    v.beta_1y,
    v.volatility_category,
    -- Sharpe Ratio calculation
    -- Measures risk-adjusted returns
    -- Higher ratio = better risk-adjusted performance
    -- Traditional interpretation:
    -- > 1: Good
    -- > 2: Very good
    -- < 0: Poor risk-adjusted returns
    (p.annualized_return - 0.02) / nullif(v.annual_volatility, 0) as sharpe_ratio,
    -- Beta calculation
    -- Measures stock's volatility compared to market
    -- Beta > 1: More volatile than market
    -- Beta < 1: Less volatile than market
    -- Beta = 1: Same volatility as market
    covar_pop(stock_return, market_return) over (
        partition by symbol 
        order by trading_date 
        rows between 251 preceding and current row
    ) / var_pop(market_return) over (
        partition by symbol 
        order by trading_date 
        rows between 251 preceding and current row
    ) as beta_1y,
    -- Distance from 52-week high/low
    ((close_price - fifty_two_week_low) / nullif(fifty_two_week_low, 0)) * 100 as pct_above_52w_low,
    ((close_price - fifty_two_week_high) / nullif(fifty_two_week_high, 0)) * 100 as pct_below_52w_high,
    -- Trading signals
    case 
        when t.rsi_14d > 70 then 'OVERBOUGHT'
        when t.rsi_14d < 30 then 'OVERSOLD'
        else 'NEUTRAL'
    end as rsi_signal,
    case 
        when t.macd_histogram > 0 and t.macd_line > 0 then 'STRONG_BUY'
        when t.macd_histogram > 0 then 'BUY'
        when t.macd_histogram < 0 and t.macd_line < 0 then 'STRONG_SELL'
        when t.macd_histogram < 0 then 'SELL'
        else 'NEUTRAL'
    end as macd_signal
from performance_calcs p
left join technical_indicators t using (symbol, trading_date)
left join volatility_metrics v using (symbol, trading_date) 