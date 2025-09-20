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
    where is_valid = true
),

daily_metrics as (
    select
        data_source,
        year,
        month,
        day,
        city,
        
        -- Temperature metrics
        avg(temperature) as avg_temperature,
        max(temperature) as max_temperature,
        min(temperature) as min_temperature,
        stddev(temperature) as temperature_stddev,
        
        -- Humidity metrics
        avg(humidity) as avg_humidity,
        max(humidity) as max_humidity,
        min(humidity) as min_humidity,
        
        -- Pressure metrics
        avg(pressure) as avg_pressure,
        max(pressure) as max_pressure,
        min(pressure) as min_pressure,
        
        -- Wind metrics
        avg(wind_speed) as avg_wind_speed,
        max(wind_speed) as max_wind_speed,
        
        -- Visibility metrics
        avg(visibility) as avg_visibility,
        min(visibility) as min_visibility,
        
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
        
        avg(temperature) as hourly_avg_temperature,
        avg(humidity) as hourly_avg_humidity,
        avg(pressure) as hourly_avg_pressure,
        avg(wind_speed) as hourly_avg_wind_speed,
        avg(visibility) as hourly_avg_visibility,
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
        
        -- Extreme weather conditions
        case 
            when max(temperature) > 35 then 'HIGH_TEMPERATURE'
            when min(temperature) < -10 then 'LOW_TEMPERATURE'
            when max(wind_speed) > 20 then 'HIGH_WIND'
            when min(visibility) < 1 then 'LOW_VISIBILITY'
            when avg(pressure) < 950 then 'LOW_PRESSURE'
            else 'NORMAL'
        end as weather_alert_type,
        
        -- Alert severity
        case 
            when max(temperature) > 40 or min(temperature) < -20 then 'SEVERE'
            when max(temperature) > 35 or min(temperature) < -10 or max(wind_speed) > 25 then 'HIGH'
            when max(temperature) > 30 or min(temperature) < -5 or max(wind_speed) > 15 then 'MEDIUM'
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
