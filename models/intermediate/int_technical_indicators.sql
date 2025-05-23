with daily_prices as (
    select * from {{ ref('int_daily_metrics') }}
),

-- Calculate RSI (Relative Strength Index)
-- RSI = 100 - (100 / (1 + RS))
-- RS = Average Gain / Average Loss over 14 periods
rsi_calc as (
    select
        symbol,
        trading_date,
        close_price,
        daily_return_pct,
        -- Calculate average gains (only positive returns)
        avg(case when daily_return_pct > 0 then daily_return_pct else 0 end) 
            over (partition by symbol order by trading_date rows between 13 preceding and current row) as avg_gain,
        -- Calculate average losses (absolute value of negative returns)
        abs(avg(case when daily_return_pct < 0 then daily_return_pct else 0 end))
            over (partition by symbol order by trading_date rows between 13 preceding and current row) as avg_loss
    from daily_prices
),

rsi as (
    select
        *,
        -- RSI calculation
        -- When avg_loss = 0, RSI = 100 (maximum strength)
        -- Otherwise, apply standard RSI formula
        case 
            when avg_loss = 0 then 100
            else 100 - (100 / (1 + (avg_gain / avg_loss)))
        end as rsi_14d
    from rsi_calc
),

-- Calculate MACD (Moving Average Convergence Divergence)
-- MACD Line = 12-day EMA - 26-day EMA
-- Signal Line = 9-day EMA of MACD Line
-- Histogram = MACD Line - Signal Line
macd_calc as (
    select
        symbol,
        trading_date,
        close_price,
        -- 12-day EMA approximation using simple moving average
        -- In production, consider using proper exponential moving average
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 11 preceding and current row
        ) as ema_12d,
        -- 26-day EMA approximation
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 25 preceding and current row
        ) as ema_26d
    from daily_prices
),

macd as (
    select
        *,
        ema_12d - ema_26d as macd_line,
        -- 9-day EMA of MACD line (Signal Line)
        avg(ema_12d - ema_26d) over (
            partition by symbol 
            order by trading_date 
            rows between 8 preceding and current row
        ) as signal_line
    from macd_calc
),

-- Calculate Bollinger Bands
-- Consists of:
-- 1. 20-day Simple Moving Average (Middle Band)
-- 2. Upper Band = SMA + (2 * Standard Deviation)
-- 3. Lower Band = SMA - (2 * Standard Deviation)
bollinger as (
    select
        symbol,
        trading_date,
        close_price,
        -- Middle Band (20-day SMA)
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as sma_20d,
        -- Standard deviation for band width
        stddev(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as std_20d
    from daily_prices
)

-- Combine all technical indicators
select
    b.*,
    r.rsi_14d,
    m.macd_line,
    m.signal_line,
    m.macd_line - m.signal_line as macd_histogram,
    -- Calculate Bollinger Bands
    b.sma_20d + (2 * b.std_20d) as upper_band,
    b.sma_20d - (2 * b.std_20d) as lower_band,
    -- Generate trading signals based on Bollinger Bands
    case 
        when close_price > (b.sma_20d + (2 * b.std_20d)) then 'OVERBOUGHT'
        when close_price < (b.sma_20d - (2 * b.std_20d)) then 'OVERSOLD'
        else 'NEUTRAL'
    end as bollinger_signal
from bollinger b
left join rsi r using (symbol, trading_date)
left join macd m using (symbol, trading_date) 