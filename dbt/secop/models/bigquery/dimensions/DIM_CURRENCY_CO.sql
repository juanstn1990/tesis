SELECT
     -1                                                                             AS ID_CURRENCY
    ,-1                                                                             AS CURRENCY_ID
    ,'_WITHOUT_STATUS'                                                              AS CURRENCY_NAME
    ,'_WITHOUT_CODE'                                                                AS CODE
    ,'_WITHOUT_SYMBOL'                                                              AS SYMBOL
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['ID_CURRENCY', 'NAME_', 'CODE']) }}       AS ID_CURRENCY
    ,ID_CURRENCY                                                                    AS CURRENCY_ID
    ,UPPER(NAME_)                                                                   AS CURRENCY_NAME
    ,UPPER(CODE)                                                                    AS CODE
    ,UPPER(SYMBOL)                                                                  AS SYMBOL
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM
    {{ source('FINKARGO_CO', 'Currency') }}
