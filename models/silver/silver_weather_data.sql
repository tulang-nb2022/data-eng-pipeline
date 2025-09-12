-- Silver Layer: Cleaned and Enriched Weather Data
-- This model reads from the bronze layer and applies data quality rules

{{ config(
    materialized='table',
    partition_by={
        "field": "processing_timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

with bronze_data as (
    select *
    from {{ source('bronze', 'weather_data') }}
    -- Note: In production, you would read from S3 Delta Lake tables
    -- For now, this assumes the data is available as a dbt source
),

cleaned_data as (
    select
        -- Core weather measurements
        temperature,
        case 
            when wind_speed is null then 0.0
            else cast(regexp_replace(wind_speed, '[^0-9.]', '') as double)
        end as wind_speed,
        city,
        case 
            when timestamp is null then processing_timestamp
            else cast(timestamp as timestamp)
        end as timestamp,
        humidity,
        pressure,
        visibility,
        
        -- Processing metadata
        processing_timestamp,
        hour,
        minute,
        data_source,
        
        -- Date partitioning
        year(processing_timestamp) as year,
        month(processing_timestamp) as month,
        day(processing_timestamp) as day,
        
        -- Data quality scoring
        case 
            when temperature is null then 0.0
            when humidity is null then 0.5
            when pressure is null then 0.5
            when wind_speed is null then 0.5
            else 1.0
        end as quality_score,
        
        -- Validation flags
        case 
            when temperature is not null 
                and humidity is not null 
                and pressure is not null 
                and wind_speed is not null
            then true
            else false
        end as is_valid,
        
        -- Kafka metadata
        kafka_offset,
        kafka_partition,
        kafka_timestamp
        
    from bronze_data
),

deduplicated_data as (
    select *
    from (
        select *,
            row_number() over (
                partition by temperature, wind_speed, city, timestamp, data_source
                order by processing_timestamp desc
            ) as rn
        from cleaned_data
    )
    where rn = 1
),

validated_data as (
    select *
    from deduplicated_data
    where is_valid = true
        and temperature between -90 and 60
        and humidity between 0 and 100
        and pressure between 800 and 1100
        and wind_speed >= 0
        and visibility >= 0
)

select
    temperature,
    wind_speed,
    city,
    timestamp,
    humidity,
    pressure,
    visibility,
    processing_timestamp,
    hour,
    minute,
    data_source,
    year,
    month,
    day,
    quality_score,
    is_valid,
    kafka_offset,
    kafka_partition,
    kafka_timestamp
    
from validated_data

order by processing_timestamp desc