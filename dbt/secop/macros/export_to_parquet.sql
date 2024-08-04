{% macro export_to_parquet(table_name) %}
    COPY (SELECT * FROM {{ this }} )
    TO 's3://data-finkargo-lz-dev/duckdb/models/{{ table_name }}.parquet'
    (FORMAT 'parquet', CODEC 'ZSTD');
{% endmacro %}
