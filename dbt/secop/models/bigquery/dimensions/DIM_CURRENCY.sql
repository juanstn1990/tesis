SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['moneda']) }}      AS id_moneda
    ,{{ var('normalize_function') }}(moneda)                AS moneda
    ,CURRENT_TIMESTAMP()                                    AS control_timestamp_creacion 
FROM 
    {{ source('secop', 'secop') }}
WHERE moneda IS NOT NULL
UNION ALL
SELECT 
    '-1'                                                    AS id_moneda
    ,'SIN_MONEDA'                                           AS moneda
    ,CURRENT_TIMESTAMP()                                    AS control_timestamp_creacion