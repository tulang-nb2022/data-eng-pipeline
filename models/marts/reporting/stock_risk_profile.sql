{{
    config(
        materialized='table',
        unique_key=['symbol', 'analysis_date']
    )
}}

with latest_metrics as (
    select * from {{ ref('fct_stock_performance') }}
    where trading_date = (select max(trading_date) from {{ ref('fct_stock_performance') }})
),

risk_profile as (
    select
        symbol,
        trading_date as analysis_date,
        beta_1y,
        annual_volatility,
        sharpe_ratio,
        case 
            when beta_1y > 1.5 then 'AGGRESSIVE'
            when beta_1y > 1.0 then 'MODERATE_AGGRESSIVE'
            when beta_1y > 0.5 then 'MODERATE'
            else 'CONSERVATIVE'
        end as risk_profile,
        case
            when sharpe_ratio > 1.0 then 'GOOD'
            when sharpe_ratio > 0 then 'MODERATE'
            else 'POOR'
        end as risk_adjusted_performance,
        case
            when rsi_signal = 'OVERBOUGHT' and macd_signal like '%SELL%' then 'HIGH_RISK'
            when rsi_signal = 'OVERSOLD' and macd_signal like '%BUY%' then 'OPPORTUNITY'
            else 'NEUTRAL'
        end as technical_signal
    from latest_metrics
)

select * from risk_profile 