# Custom Macros

## Technical Analysis Macros

### calculate_moving_average
Calculates moving average with flexible window size.

Parameters:
- column_name: Column to average
- window_days: Number of days in moving window

Example:
```sql
{{ calculate_moving_average('close_price', 20) }}
```

### calculate_volatility
Calculates price volatility over specified period.

Parameters:
- return_column: Daily returns column
- window_days: Analysis window
- annualize: Boolean to annualize result 