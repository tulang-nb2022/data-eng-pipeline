with source as (
    select * from {{ source('alpha_vantage', 'raw_daily_prices') }}
),

renamed as (
    select
        symbol,
        date::date as trading_date,
        open::float as open_price,
        high::float as high_price,
        low::float as low_price,
        close::float as close_price,
        volume::integer as trading_volume,
        _loaded_at
    from source
)

select * from renamed 