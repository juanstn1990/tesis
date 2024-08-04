SELECT
     '-1'                                                                           AS ID_FREIGHT_FORWARDER
    ,'-1'                                                                           AS ID_COMPANY
    ,'_WITHOUT_FREIGHT_FORWARDERS'                                                  AS FREIGHT_FORWARDER
    ,'_WITHOUT_NIT'                                                                 AS NIT
    ,'_WITHOUT_NUMBER_CEO'                                                          AS NUMBER_CEO
    ,LOWER('_WITHOUT_EMAIL')                                                        AS EMAIL
    ,'_WITHOUT_CUODE_ADDRESS'                                                       AS ADDRESS
    ,'_WITHOUT_MERCHANT_ACCOUNT'                                                    AS MERCHANT_ACCOUNT
    ,'_WITHOUT_NAME_CONTACT'                                                        AS NAME_CONTACT
    ,'_WITHOUT_PHONE'                                                               AS PHONE_NUMBER
    ,'_WITHOUT_CELLPHONE'                                                           AS CELLPHONE_NUMBER
    ,'2099-12-31'                                                                   AS FOUNDED_AT
    ,'_WITHOUT_DOMAIN'                                                              AS DOMAIN
    ,LOWER('_WITHOUT_WEBSITE')                                                      AS WEBSITE
    ,'-1'                                                                           AS SCORING
    ,'_WITHOUT_COUNTRY'                                                             AS COUNTRY
    ,'_WITHOUT_STATUS'                                                              AS STATUS
    , '-1'                                                                          AS SARLAFT
    ,'_WITHOUT_ECONOMIC_ACTIVITY'                                                   AS ECONOMIC_ACTIVITY
    ,'2099-12-31'                                                                   AS CREATED_AT
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
UNION
SELECT
     {{ dbt_utils.generate_surrogate_key(['PFC.ID_COMPANY']) }}                     AS ID_FREIGHT_FORWARDER
    ,PFC.ID_COMPANY                                                                 AS ID_COMPANY
    ,UPPER(PFC.NAME_)                                                               AS FREIGHT_FORWARDER
    ,PFC.NIT                                                                        AS NIT
    ,PFC.NUMBER_CEO                                                                 AS NUMBER_CEO
    ,LOWER(COALESCE(PFC.EMAIL, '_WITHOUT_EMAIL'))                                   AS EMAIL
    ,UPPER(COALESCE(PFC.ADDRESS, '_WITHOUT_ADDRESS'))                               AS ADDRESS
    ,COALESCE(PFC.MERCHANT_ACCOUNT, '_WITHOUT_MERCHANT_ACCOUNT')                    AS MERCHANT_ACCOUNT
    ,LISTAGG(DISTINCT COALESCE(UPPER(CONCAT(RLFF.NAME_,' ',RLFF.SURNAME)), '_WITHOUT_NAME_CONTACT'), ', ')
                                                                                    AS NAME_CONTACT
    ,COALESCE(NULLIF(PFC.PHONE_NUMBER, ''), '_WITHOUT_PHONE')                       AS PHONE_NUMBER
    ,COALESCE(NULLIF(PFC.CELLPHONE_NUMBER, ''), '_WITHOUT_CELLPHONE')               AS CELLPHONE_NUMBER
    ,COALESCE(PFC.FOUNDED_AT, '2099-12-31')                                         AS FOUNDED_AT
    ,UPPER(COALESCE(NULLIF(PFC.DOMAIN, ''), '_WITHOUT_DOMAIN'))
                                                                                    AS DOMAIN
    ,LOWER(COALESCE(NULLIF(PFC.WEBSITE, ''), '_WITHOUT_WEBSITE'))                   AS WEBSITE
    ,COALESCE(PFC.SCORING, '-1')                                                    AS SCORING
    ,COALESCE(PFCO.NAME_ , '_WITHOUT_COUNTRY')                                      AS COUNTRY
    ,TRANSLATE(UPPER(COALESCE(DS.NAME_ , '_WITHOUT_STATUS')), 'ÁÉÍÓÚÜ', 'AEIOUU')   AS STATUS
    ,COALESCE(PFC.SARLAFT, '-1')                                                    AS SARLAFT
    ,COALESCE(PFE.NAME_ , '_WITHOUT_ECONOMIC_ACTIVITY')                             AS ECONOMIC_ACTIVITY
    ,COALESCE(PFC.CREATED_AT, '2099-12-31' )                                        AS CREATED_AT
    ,PFC.EXTRACTED_AT_UTC                                                           AS CONTROL_TIMESTAMP_CREATED
    ,CURRENT_TIMESTAMP                                                              AS CONTROL_TIMESTAMP_UPDATED
FROM
    {{ source('FINKARGO_CO', 'Company') }} PFC
    LEFT JOIN {{ source('FINKARGO_CO', 'Status') }} DS
        ON PFC.STATUS = DS.ID_STATUS 
    LEFT JOIN {{ source('FINKARGO_CO', 'Country') }} PFCO
        ON PFC.COUNTRY =  PFCO.ID_COUNTRY
    LEFT JOIN {{ source('FINKARGO_CO', 'Economic') }} PFE 
        ON PFC.ECONOMIC_ACTIVITY = PFE.ID_ECONOMIC
    LEFT JOIN {{ source('FINKARGO_CO', 'Associate') }} RLFF 
        ON PFC.ID_COMPANY = RLFF.COMPANY
WHERE
    IS_FORWARDER = TRUE
GROUP BY 
    1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21