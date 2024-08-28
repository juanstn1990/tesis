WITH ENTIDADES AS
(
SELECT 
    {{ var('normalize_function') }}(nivel_entidad)                  AS nivel_entidad
    ,orden_entidad                                                  AS orden_entidad  
    ,{{ var('normalize_function') }}(nombre_entidad)                AS nombre_entidad
    ,SPLIT(nit_de_la_entidad, '-')[OFFSET(0)]                       AS nit_entidad
    ,c_digo_de_la_entidad                                           AS codigo_de_la_entidad
    ,{{ var('normalize_function') }}(ciudad_entidad)                AS municipio_entidad
    ,{{ var('normalize_function') }}(departamento_entidad)          AS departamento_entidad
    ,COUNT(*) AS conteo
FROM
    {{ source('secop', 'secop') }}
GROUP BY 1,2,3,4,5,6,7
)


WITH ENTIDADES_SECOP2 AS
(
SELECT 
    'SIN_NIVEL_ENTIDAD'                                             AS nivel_entidad
    ,'SIN_ORDEN_ENTIDAD'                                            AS orden_entidad  
    ,{{ var('normalize_function') }}(entidad)                       AS nombre_entidad
    ,SPLIT(nit_entidad, '-')[OFFSET(0)]                             AS nit_entidad
    ,'SIN_CODIGO'                                                   AS codigo_de_la_entidad
    ,{{ var('normalize_function') }}(municipio_entidad)             AS municipio_entidad
    ,{{ var('normalize_function') }}(departamento_entidad)          AS departamento_entidad
    ,COUNT(*) AS conteo
FROM
    {{ source('secop', 'secop_2') }}
GROUP BY 1,2,3,4,5,6,7
)



,ALL_ENTITIES AS
(

SELECT 
    {{ dbt_utils.generate_surrogate_key(['codigo_de_la_entidad']) }}
                                                                    AS id_entidad
    ,nivel_entidad                                                  AS nivel_entidad                                      
    ,CASE
        WHEN orden_entidad = 'No Definido' THEN 'SIN_ORDEN_ENTIDAD'
        ELSE orden_entidad
    END                                                             AS orden_entidad
    ,nombre_entidad                                                 AS nombre_entidad
    ,nit_entidad                                                    AS nit_entidad                            
    ,codigo_de_la_entidad                                           AS codigo_de_la_entidad
    ,DG.ciudad                                                      AS municipio_entidad      
    ,DG.departamento                                                AS departamento_entidad
    ,DG.id_geografia                                                AS id_geografia
    ,1                                                              AS source
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion 
FROM 
    ENTIDADES ENT
LEFT JOIN {{ ref('DIM_GEOGRAPHY') }} DG
    ON ENT.departamento_entidad = DG.departamento
    AND ENT.municipio_entidad = DG.ciudad
WHERE 
    codigo_de_la_entidad IS NOT NULL
QUALIFY
	ROW_NUMBER() OVER(PARTITION BY codigo_de_la_entidad ORDER BY conteo DESC) = 1

UNION ALL
SELECT 
    '-1'                                                            AS id_entidad
    ,'SIN_NIVEL_ENTIDAD'                                            AS nivel_entidad
    ,'SIN_ORDEN_ENTIDAD'                                            AS orden_entidad
    ,'SIN_NOMBRE'                                                   AS nombre_entidad
    ,'SIN_NIT'                                                      AS nit_entidad
    ,'SIN_CODIGO'                                                   AS codigo_de_la_entidad
    ,'SIN_MUNICIPIO'                                                AS municipio_entidad
    ,'SIN_DEPARTAMENTO'                                             AS departamento_entidad
    ,'-1'                                                           AS id_geografia
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion

UNION ALL

SELECT 
    {{ dbt_utils.generate_surrogate_key(['codigo_de_la_entidad']) }}
                                                                    AS id_entidad
    ,nivel_entidad                                                  AS nivel_entidad                                      
    ,CASE
        WHEN orden_entidad = 'No Definido' THEN 'SIN_ORDEN_ENTIDAD'
        ELSE orden_entidad
    END                                                             AS orden_entidad
    ,nombre_entidad                                                 AS nombre_entidad
    ,nit_entidad                                                    AS nit_entidad                            
    ,codigo_de_la_entidad                                           AS codigo_de_la_entidad
    ,DG.ciudad                                                      AS municipio_entidad      
    ,DG.departamento                                                AS departamento_entidad
    ,DG.id_geografia                                                AS id_geografia
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion 
FROM 
    ENTIDADES_SECOP2 ENT
LEFT JOIN {{ ref('DIM_GEOGRAPHY') }} DG
    ON ENT.departamento_entidad = DG.departamento
    AND ENT.municipio_entidad = DG.ciudad
WHERE 
    codigo_de_la_entidad IS NOT NULL
QUALIFY
	ROW_NUMBER() OVER(PARTITION BY codigo_de_la_entidad ORDER BY conteo DESC) = 1
)
