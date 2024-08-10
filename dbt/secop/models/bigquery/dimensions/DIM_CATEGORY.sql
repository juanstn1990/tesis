SELECT 
    {{ dbt_utils.generate_surrogate_key(['id_clase']) }}            AS id_categoria
    ,id_grupo                                                       AS id_grupo                
    ,{{ var('normalize_function') }}(nombre_grupo)                  AS nombre_grupo
    ,id_familia                                                     AS id_familia
    ,{{ var('normalize_function') }}(nombre_familia)                AS nombre_familia
    ,id_clase                                                       AS id_clase
    ,{{ var('normalize_function') }}(nombre_clase)                  AS nombre_clase
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion 
FROM
    {{ source('secop', 'secop') }}
WHERE id_clase IS NOT NULL
QUALIFY
	ROW_NUMBER() OVER(PARTITION BY id_clase ORDER BY fecha_de_cargue_en_el_secop desc) =1 
UNION ALL   
SELECT 
    '-1'                                                            AS id_categoria
    ,'SIN_ID_GRUPO'                                                 AS id_grupo
    ,'SIN_NOMBRE_GRUPO'                                             AS nombre_grupo
    ,'SIN_ID_FAMILIA'                                                AS id_familia
    ,'SIN_NOMBRE_FAMILIA'                                           AS nombre_familia
    ,'SIN_ID_CLASE'                                                 AS id_clase
    ,'SIN_NOMBRE_CLASE'                                             AS nombre_clase
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion 

