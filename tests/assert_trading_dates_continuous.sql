-- Custom test to ensure no missing trading dates
with date_gaps as (
    select
        symbol,
        trading_date,
        lead(trading_date) over (partition by symbol order by trading_date) as next_date,
        extract(days from lead(trading_date) over (partition by symbol order by trading_date) - trading_date) as days_difference
    from {{ ref('fct_daily_trading') }}
)

select *
from date_gaps
where 
    days_difference > 3  -- Allow for weekends and holidays
    and extract(dow from trading_date) between 1 and 5  -- Only check weekdays 