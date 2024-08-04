WITH PROVIDERS AS (
    SELECT 
    DISTINCT 
        CLEAN_PROVIDERS(FPR.NAME_)                                           AS PROVIDER_NAME
        ,LAST_VALUE(ID_PROVIDER) OVER(PARTITION BY PROVIDER_NAME ORDER BY created_at)
                                                                                    AS ID_PROVIDER
        ,ARRAY_DISTINCT(ARRAY_AGG(ID_PROVIDER) OVER(PARTITION BY PROVIDER_NAME))
                                                                                    AS PROVIDER_IDS  
    FROM  {{ source('FINKARGO_CO', 'Provider') }} FPR
    QUALIFY
        ROW_NUMBER() OVER(PARTITION BY PROVIDER_NAME ORDER BY ID_PROVIDER DESC) = 1
)
SELECT
     '-1'                                                                           AS ID_PROVIDER
    ,ARRAY[-1]                                                                      AS PROVIDER_IDS
    ,'_WITHOUT_PROVIDER_NAME'                                                       AS PROVIDER_NAME
    ,'_WITHOUT_ADDRESS'                                                             AS ADDRESS
    ,'_WITHOUT_CITY'                                                                AS CITY
    ,'_WITHOUT_STATE'                                                               AS STATE
    ,'_WITHOUT_COUNTRY'                                                             AS COUNTRY
    ,'_WITHOUT_SEGMENT'                                                             AS PROVIDER_SEGMENT
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['FPR.ID_PROVIDER']) }}                    AS ID_PROVIDER
    ,P.PROVIDER_IDS                                                                 AS PROVIDER_IDS
    ,UPPER(FPR.NAME_)                                                               AS PROVIDER_NAME
    ,UPPER(COALESCE(NULLIF(FPR.ADDRESS, ''), '_WITHOUT_ADDRESS'))                   AS ADDRESS
    ,UPPER(COALESCE(
        CASE LOWER(CIT.NAME_)
            WHEN 'sin definir' THEN NULL
            ELSE NULLIF(CIT.NAME_, '')
        END, 
        '_WITHOUT_CITY'))                                                           AS CITY
    ,'_WITHOUT_STATE'                                                               AS STATE
    ,UPPER(COALESCE(
        CASE 
            WHEN LOWER(CIT.NAME_) = 'sin definir' THEN NULL
            ELSE NULLIF(COU.NAME_, '')
        END,
        '_WITHOUT_COUNTRY'))                                                        AS COUNTRY
    ,'_WITHOUT_SEGMENT'                                                             AS PROVIDER_SEGMENT
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM 
    {{ source('FINKARGO_CO', 'Provider') }} FPR
    JOIN PROVIDERS P 
        ON FPR.ID_PROVIDER  = P.ID_PROVIDER 
    LEFT JOIN {{ source('FINKARGO_CO', 'City') }} CIT 
        ON FPR.CITY = CIT.ID_CITY
    LEFT JOIN {{ source('FINKARGO_CO', 'Country') }} COU 
        ON CIT.COUNTRY = COU.ID_COUNTRY