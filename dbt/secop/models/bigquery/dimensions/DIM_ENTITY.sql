WITH ENTIDADES AS
(
SELECT 
    {{ var('normalize_function') }}(nivel_entidad)                  AS nivel_entidad
    ,orden_entidad                                                  AS orden_entidad  
    ,{{ var('normalize_function') }}(nombre_entidad)                AS nombre_entidad
    ,SPLIT(nit_de_la_entidad, '-')[OFFSET(0)]                       AS nit_entidad
    ,c_digo_de_la_entidad                                           AS codigo_de_la_entidad
    ,{{ var('normalize_function') }}(municipio_entidad)             AS municipio_entidad
    ,{{ var('normalize_function') }}(departamento_entidad)          AS departamento_entidad
    ,COUNT(*) AS conteo
FROM
    {{ source('secop', 'secop') }}
GROUP BY 1,2,3,4,5,6,7
)
,ENTIDADES_SECOP2 AS
(
SELECT 
    'SIN_NIVEL_ENTIDAD'                                             AS nivel_entidad
    ,'SIN_ORDEN_ENTIDAD'                                            AS orden_entidad  
    ,{{ var('normalize_function') }}(entidad)                       AS nombre_entidad
    ,SPLIT(nit_entidad, '-')[OFFSET(0)]                             AS nit_entidad
    ,'SIN_CODIGO'                                                   AS codigo_de_la_entidad
    ,CASE
        WHEN {{ var('normalize_function') }}(departamento_entidad) IN ('DISTRITO CAPITAL DE BOGOTA') THEN 'BOGOTA D.C.'
        WHEN {{ var('normalize_function') }}(ciudad_entidad) IN ('NO DEFINIDO') THEN 'SIN_CIUDAD'
        WHEN {{ var('normalize_function') }}(ciudad_entidad) IN ('CUCUTA')  AND {{ var('normalize_function') }}(departamento_entidad)='NORTE DE SANTANDER' THEN 'SAN JOSE DE CUCUTA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('LEGUIZAMO')  AND {{ var('normalize_function') }}(departamento_entidad)='PUTUMAYO' THEN 'PUERTO LEGUIZAMO'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('VILLAGARZON')  AND {{ var('normalize_function') }}(departamento_entidad)='PUTUMAYO' THEN 'VILLA GARZON/VILLA AMAZONICA' 
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('VALLE DEL GUAMUEZ')  AND {{ var('normalize_function') }}(departamento_entidad)='PUTUMAYO' THEN 'VALLE DEL GUAMUEZ/LA HORMIGA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('CUASPUD')  AND {{ var('normalize_function') }}(departamento_entidad)='NARINO' THEN 'CUASPUD/CARLOSAMA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('ARIGUANI')  AND {{ var('normalize_function') }}(departamento_entidad)='MAGDALENA' THEN 'ARIGUANI/EL DIFICIL'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('CAROLINA')  AND {{ var('normalize_function') }}(departamento_entidad)='ANTIOQUIA' THEN 'CAROLINA DEL PRINCIPE'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('ESPINAL')  AND {{ var('normalize_function') }}(departamento_entidad)='TOLIMA' THEN 'EL ESPINAL'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('GUAMO')  AND {{ var('normalize_function') }}(departamento_entidad)='TOLIMA' THEN 'EL GUAMO'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('LA PLAYA')  AND {{ var('normalize_function') }}(departamento_entidad)='NORTE DE SANTANDER' THEN 'LA PLAYA DE BELEN'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('LIBANO')  AND {{ var('normalize_function') }}(departamento_entidad)='TOLIMA' THEN 'EL LIBANO'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('LOPEZ')  AND {{ var('normalize_function') }}(departamento_entidad)='CAUCA' THEN 'LOPEZ DE MICAY'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('MANAURE')  AND {{ var('normalize_function') }}(departamento_entidad)='CESAR' THEN 'MANAURE BALCON DEL CESAR'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('MARIQUITA')  AND {{ var('normalize_function') }}(departamento_entidad)='TOLIMA' THEN 'SAN SEBASTIAN DE MARIQUITA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('MOCOA')  AND {{ var('normalize_function') }}(departamento_entidad)='PUTUMAYO' THEN 'SAN MIGUEL DE MOCOA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('PAEZ')  AND {{ var('normalize_function') }}(departamento_entidad)='CAUCA' THEN 'PAEZ/BELALCAZAR'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('PENOL')  AND {{ var('normalize_function') }}(departamento_entidad)='ANTIOQUIA' THEN 'EL PENOL'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('PITAL')  AND {{ var('normalize_function') }}(departamento_entidad)='HUILA' THEN 'EL PITAL'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('RETIRO')  AND {{ var('normalize_function') }}(departamento_entidad)='ANTIOQUIA' THEN 'EL RETIRO'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('ROBERTO PAYAN')  AND {{ var('normalize_function') }}(departamento_entidad)='NARINO' THEN 'ROBERTO PAYAN/SAN JOSE'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('SALAZAR')  AND {{ var('normalize_function') }}(departamento_entidad)='NORTE DE SANTANDER' THEN 'SALAZAR DE LAS PALMAS'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('SAN ANDRES')  AND {{ var('normalize_function') }}(departamento_entidad)='ANTIOQUIA' THEN 'SAN ANDRES DE CUERQUIA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('SAN ESTANISLAO')  AND {{ var('normalize_function') }}(departamento_entidad)='BOLIVAR' THEN 'SAN ESTANISLAO DE KOSTKA'
        when {{ var('normalize_function') }}(ciudad_entidad) IN ('SAN MIGUEL')  AND {{ var('normalize_function') }}(departamento_entidad)='PUTUMAYO' THEN 'SAN MIGUEL (LA DORADA)'
        ELSE 'SIN_CIUDAD'
    END                                                             AS municipio_entidad
    ,CASE
     WHEN {{ var('normalize_function') }}(departamento_entidad) IN ('DISTRITO CAPITAL DE BOGOTA') THEN 'BOGOTA D.C.'
     WHEN {{ var('normalize_function') }}(departamento_entidad) IN ('NO DEFINIDO') THEN 'SIN_DEPARTAMENTO'
     ELSE {{ var('normalize_function') }}(departamento_entidad)
    END                                                             AS departamento_entidad
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
    ,ENT.municipio_entidad                                          AS municipio_entidad_original
    ,ENT.departamento_entidad                                       AS departamento_entidad_original
    ,DG.ciudad                                                      AS municipio_entidad      
    ,DG.departamento                                                AS departamento_entidad
    ,DG.id_geografia                                                AS id_geografia
    ,1                                                              AS source
    ,'secop 1'                                                      AS source_name
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
    ,'SIN_MUNICIPIO'                                                AS municipio_entidad_original
    ,'SIN_DEPARTAMENTO'                                             AS departamento_entidad_original
    ,'SIN_MUNICIPIO'                                                AS municipio_entidad
    ,'SIN_DEPARTAMENTO'                                             AS departamento_entidad
    ,'-1'                                                           AS id_geografia
    ,1                                                              AS source
    ,'secop 1'                                                      AS source_name
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
    ,ENT.municipio_entidad                                          AS municipio_entidad_original
    ,ENT.departamento_entidad                                       AS departamento_entidad_original
    ,DG.ciudad                                                      AS municipio_entidad      
    ,DG.departamento                                                AS departamento_entidad
    ,DG.id_geografia                                                AS id_geografia
    ,2                                                              AS source
    ,'secop 2'                                                      AS source_name
    ,CURRENT_TIMESTAMP()                                            AS control_timestamp_creacion 
FROM 
    ENTIDADES_SECOP2 ENT
LEFT JOIN {{ ref('DIM_GEOGRAPHY') }} DG
    ON ENT.departamento_entidad = DG.departamento
    AND ENT.municipio_entidad = DG.ciudad
WHERE 
    codigo_de_la_entidad IS NOT NULL
QUALIFY
	ROW_NUMBER() OVER(PARTITION BY nit_entidad ORDER BY conteo DESC) = 1
)
SELECT * FROM ALL_ENTITIES
QUALIFY
    ROW_NUMBER() OVER(PARTITION BY nit_entidad ORDER BY source) = 1