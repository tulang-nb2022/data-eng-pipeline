{% macro calculate_moving_average(column_name, window_days) %}
    avg({{ column_name }}) over (
        partition by symbol 
        order by trading_date 
        rows between {{ window_days - 1 }} preceding and current row
    )
{% endmacro %} 