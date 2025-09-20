-- Gold Layer: Aggregated Weather Metrics
-- This model creates business-ready aggregated metrics from the silver layer

{{ config(
    materialized='table',
    pre_hook='INSTALL httpfs; LOAD httpfs;',
    post_hook=["
        INSTALL httpfs; LOAD httpfs;
        SET s3_region='us-east-1';
        SET s3_use_ssl=true;
        SET s3_access_key_id='{{ env_var('aws_access_key_id') }}';
        SET s3_secret_access_key='{{ env_var('aws_secret_access_key') }}';
        COPY (SELECT * FROM {{ this }})
        TO 's3://data-eng-bucket-345/gold/weather/'
        (
            FORMAT PARQUET,
            OVERWRITE_OR_IGNORE true,
            COMPRESSION SNAPPY,
            PARTITION_BY (year, month, day)
        )
    "]
) }}

with silver_data as (
    select *
    from read_parquet('s3://data-eng-bucket-345/silver/weather/**/*.parquet')
    -- Remove strict validation - let Great Expectations handle data quality
    -- where is_valid = true
),

daily_metrics as (
    select
        data_source,
        year,
        month,
        day,
        city,
        
        -- Temperature metrics (handle nulls gracefully)
        avg(case when temperature is not null and temperature != 0 then temperature end) as avg_temperature,
        max(case when temperature is not null and temperature != 0 then temperature end) as max_temperature,
        min(case when temperature is not null and temperature != 0 then temperature end) as min_temperature,
        stddev(case when temperature is not null and temperature != 0 then temperature end) as temperature_stddev,
        
        -- Humidity metrics (handle nulls and zeros gracefully)
        avg(case when humidity is not null and humidity > 0 then humidity end) as avg_humidity,
        max(case when humidity is not null and humidity > 0 then humidity end) as max_humidity,
        min(case when humidity is not null and humidity > 0 then humidity end) as min_humidity,
        
        -- Pressure metrics (handle nulls and zeros gracefully)
        avg(case when pressure is not null and pressure > 0 then pressure end) as avg_pressure,
        max(case when pressure is not null and pressure > 0 then pressure end) as max_pressure,
        min(case when pressure is not null and pressure > 0 then pressure end) as min_pressure,
        
        -- Wind metrics (handle nulls gracefully)
        avg(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) as avg_wind_speed,
        max(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) as max_wind_speed,
        
        -- Visibility metrics (handle nulls gracefully)
        avg(case when visibility is not null and visibility >= 0 then visibility end) as avg_visibility,
        min(case when visibility is not null and visibility >= 0 then visibility end) as min_visibility,
        
        -- Quality metrics
        avg(quality_score) as avg_quality_score,
        count(*) as record_count,
        
        -- Data freshness
        max(processing_timestamp) as latest_processing_timestamp,
        min(processing_timestamp) as earliest_processing_timestamp
        
    from silver_data
    group by data_source, year, month, day, city
),

hourly_metrics as (
    select
        data_source,
        year,
        month,
        day,
        hour,
        city,
        
        avg(case when temperature is not null and temperature != 0 then temperature end) as hourly_avg_temperature,
        avg(case when humidity is not null and humidity > 0 then humidity end) as hourly_avg_humidity,
        avg(case when pressure is not null and pressure > 0 then pressure end) as hourly_avg_pressure,
        avg(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) as hourly_avg_wind_speed,
        avg(case when visibility is not null and visibility >= 0 then visibility end) as hourly_avg_visibility,
        avg(quality_score) as hourly_avg_quality_score,
        count(*) as hourly_record_count
        
    from silver_data
    group by data_source, year, month, day, hour, city
),

weather_alerts as (
    select
        data_source,
        year,
        month,
        day,
        city,
        
        -- Extreme weather conditions (more lenient thresholds)
        case 
            when max(case when temperature is not null and temperature != 0 then temperature end) > 35 then 'HIGH_TEMPERATURE'
            when min(case when temperature is not null and temperature != 0 then temperature end) < -10 then 'LOW_TEMPERATURE'
            when max(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) > 20 then 'HIGH_WIND'
            when min(case when visibility is not null and visibility >= 0 then visibility end) < 1 then 'LOW_VISIBILITY'
            when avg(case when pressure is not null and pressure > 0 then pressure end) < 950 then 'LOW_PRESSURE'
            else 'NORMAL'
        end as weather_alert_type,
        
        -- Alert severity (more lenient thresholds)
        case 
            when max(case when temperature is not null and temperature != 0 then temperature end) > 40 or min(case when temperature is not null and temperature != 0 then temperature end) < -20 then 'SEVERE'
            when max(case when temperature is not null and temperature != 0 then temperature end) > 35 or min(case when temperature is not null and temperature != 0 then temperature end) < -10 or max(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) > 25 then 'HIGH'
            when max(case when temperature is not null and temperature != 0 then temperature end) > 30 or min(case when temperature is not null and temperature != 0 then temperature end) < -5 or max(case when wind_speed is not null and wind_speed >= 0 then wind_speed end) > 15 then 'MEDIUM'
            else 'LOW'
        end as alert_severity
        
    from silver_data
    group by data_source, year, month, day, city
)

select
    dm.*,
    wa.weather_alert_type,
    wa.alert_severity,
    {{ dbt.current_timestamp() }} as gold_processing_timestamp
    
from daily_metrics dm
left join weather_alerts wa 
    on dm.data_source = wa.data_source 
    and dm.year = wa.year 
    and dm.month = wa.month 
    and dm.day = wa.day 
    and dm.city = wa.city

order by dm.data_source, dm.year, dm.month, dm.day, dm.city
