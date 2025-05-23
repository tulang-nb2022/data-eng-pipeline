-- Test that data is updated by 9am EST
select
    symbol,
    max(trading_date) as last_trading_date
from {{ ref('stock_daily_summary') }}
group by symbol
having max(trading_date) < current_date - 1
    and extract(hour from current_timestamp at time zone 'EST') >= 9 