select 
    {{ dbt_utils.generate_surrogate_key(['id_objeto_a_contratar']) }}           AS id_objeto_a_contratar
    ,id_objeto_a_contratar                                                      AS id_objeto_a_contratar_secop                                     
    ,{{ var('normalize_function') }}(objeto_a_contratar)                        AS objeto_a_contratar
    ,CURRENT_TIMESTAMP()                                                        AS control_timestamp_creacion 
FROM 
    {{ source('secop', 'secop') }}
QUALIFY 
	ROW_NUMBER() OVER(PARTITION BY id_objeto_a_contratar ORDER BY  id_objeto_a_contratar DESC) = 1
UNION ALL
SELECT 
    '-1'                                                                        AS id_contratista
    ,'SIN_ID_OBJETO_A_CONTRATAR'                                                AS id_objeto_a_contratar_secop
    ,'SIN_OBJETO_A_CONTRATAR'                                                   AS objeto_a_contratar
    ,CURRENT_TIMESTAMP()                                                        AS control_timestamp_creacion