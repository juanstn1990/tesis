SELECT
     '-1'                                                                           AS ID_STATUS
    ,'_WITHOUT_CODE'                                                                AS STATUS_CODE
    ,'_WITHOUT_STATUS'                                                              AS STATUS_NAME
    ,'_WITHOUT_CATEGORY'                                                            AS STATUS_CATEGORY
    ,'_WITHOUT_DB_SOURCE'                                                           AS DB_SOURCE
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['CODE', 'NAME_', 'CATEGORY']) }}          AS ID_STATUS
    ,UPPER(CODE)                                                                    AS STATUS_CODE
    ,UPPER(NAME_)                                                                   AS STATUS_NAME
    ,UPPER(CATEGORY)                                                                AS STATUS_CATEGORY
    ,'FINKARGO'                                                                     AS DB_SOURCE
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM
    {{ source('FINKARGO_CO', 'Status') }}
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['CODE', 'NAME_', 'CATEGORY']) }}          AS ID_STATUS
    ,UPPER(CODE)                                                                    AS STATUS_CODE
    ,UPPER(NAME_)                                                                   AS STATUS_NAME
    ,UPPER(CATEGORY)                                                                AS STATUS_CATEGORY
    ,'PORTFOLIO'                                                                    AS DB_SOURCE
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM
    {{ source('PORTFOLIO_CO', 'status') }}
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['SUB.CODE', 'SUB.NAME_', 'STA.CATEGORY']) }}
                                                                                    AS ID_STATUS
    ,UPPER(SUB.CODE)                                                                AS STATUS_CODE
    ,UPPER(SUB.NAME_)                                                               AS STATUS_NAME
    ,UPPER(STA.CATEGORY)                                                            AS STATUS_CATEGORY
    ,'PORTFOLIO'                                                                    AS DB_SOURCE
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM
    {{ source('PORTFOLIO_CO', 'substatus') }}  SUB
    LEFT JOIN {{ source('PORTFOLIO_CO', 'status') }} STA
        ON SUB.STATUS_ID = STA.ID_STATUS
