{% macro upload_to_s3(table_name, s3_path) %}
    {% set upload_sql %}
        INSTALL httpfs; LOAD httpfs;
        SET s3_region='us-east-1';
        SET s3_use_ssl=true;
        SET s3_access_key_id='{{ env_var('AWS_ACCESS_KEY_ID') }}';
        SET s3_secret_access_key='{{ env_var('AWS_SECRET_ACCESS_KEY') }}';
        COPY (SELECT * FROM {{ table_name }})
        TO '{{ s3_path }}'
        (
            FORMAT PARQUET,
            OVERWRITE_OR_IGNORE true,
            COMPRESSION SNAPPY,
            PARTITION_BY (year, month, day)
        )
    {% endset %}
    
    {% do run_query(upload_sql) %}
{% endmacro %}
