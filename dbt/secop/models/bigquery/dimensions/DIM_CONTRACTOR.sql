WITH CONTRATISTAS AS
(
    SELECT 
    tipo_identifi_del_contratista                                                   AS tipo_identifi_del_contratista
    ,REGEXP_EXTRACT(SPLIT(identificacion_del_contratista, '-')[OFFSET(0)], r'\d+')  AS identificacion_del_contratista
    ,nom_razon_social_contratista                                                   AS nom_razon_social_contratista                            
    ,dpto_y_muni_contratista                                                        AS dpto_y_muni_contratista
    ,COUNT(*)                                                                       AS conteo_
    FROM
        {{ source('secop', 'secop') }}
    GROUP BY 1,2,3,4
),
ULTIMO_REPRESENTANTE AS
(
SELECT 
REGEXP_EXTRACT(SPLIT(identificacion_del_contratista, '-')[OFFSET(0)], r'\d+')   AS identificacion_del_contratista_mod 
,tipo_doc_representante_legal                                                   AS tipo_doc_representante_legal
,nombre_del_represen_legal                                                      AS nombre_del_represen_legal                                           
,identific_representante_legal                                                  AS identific_representante_legal
,sexo_replegal                                                                  AS sexo_replegal
,CAST(TIMESTAMP(fecha_de_cargue_en_el_secop) AS DATE)                           AS fecha_cargue
FROM 
`artful-sled-419501`.secop.secop
QUALIFY
	ROW_NUMBER() OVER(PARTITION BY identificacion_del_contratista_mod order by fecha_cargue desc) = 1
)

,CONTRATISTAS_MOD AS
(
    SELECT
        tipo_identifi_del_contratista                                               AS tipo_identifi_del_contratista
        ,identificacion_del_contratista                                             AS identificacion_del_contratista
        ,COALESCE(nom_razon_social_contratista,'SIN_NOMBRE_CONTRATISTA')            AS nom_razon_social_contratista
        ,COALESCE(dpto_y_muni_contratista,'-1')                                     AS dpto_y_muni_contratista
    FROM 
    CONTRATISTAS
    QUALIFY 
        ROW_NUMBER() OVER(PARTITION BY identificacion_del_contratista ORDER BY conteo_ desc)= 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['identificacion_del_contratista']) }}          AS id_contratista
    ,{{ var('normalize_function') }}(tipo_identifi_del_contratista)                     AS tipo_identifi_del_contratista
    ,identificacion_del_contratista                                                     AS identificacion_del_contratista
    ,{{ var('normalize_function') }}(nom_razon_social_contratista)                      AS nom_razon_social_contratista
    ,COALESCE(DG1.departamento,'SIN_DEPARTAMENTO')                                      AS dpto_y_muni_contratista
    ,{{ var('normalize_function') }}(UR.tipo_doc_representante_legal)                   AS ultimo_tipo_doc_representante_legal
    ,{{ var('normalize_function') }}(UR.nombre_del_represen_legal)                      AS ultimo_nombre_del_represen_legal
    ,{{ var('normalize_function') }}(UR.identific_representante_legal)                  AS ultimo_identific_representante_legal
    ,{{ var('normalize_function') }}(UR.sexo_replegal)                                  AS ultimo_sexo_replegal
    ,COALESCE(DG1.id_geografia,'-1')                                                    AS id_geografia
    ,CURRENT_TIMESTAMP()                                                                AS control_timestamp_creacion                                                                                       
FROM CONTRATISTAS_MOD
    LEFT JOIN {{ ref('DIM_GEOGRAPHY') }} DG1
        ON {{ var('normalize_function') }}(dpto_y_muni_contratista) = DG1.departamento
        AND DG1.tipo_geografia = 'DEPARTAMENTO'
    LEFT JOIN ULTIMO_REPRESENTANTE UR
        ON CONTRATISTAS_MOD.identificacion_del_contratista = UR.identificacion_del_contratista_mod
WHERE
    identificacion_del_contratista IS NOT NULL
UNION ALL
SELECT 
    '-1'                                                                                AS id_contratista
    ,'SIN_TIPO_IDENTIFICACION'                                                          AS tipo_identifi_del_contratista
    ,'SIN_IDENTIFICACION'                                                               AS identificacion_del_contratista
    ,'SIN_NOMBRE_CONTRATISTA'                                                           AS nom_razon_social_contratista
    ,'SIN_DEPARTAMENTO'                                                                 AS dpto_y_muni_contratista
    ,'SIN_TIPO_DOC_REPRESENTANTE'                                                       AS ultimo_tipo_doc_representante_legal
    ,'SIN_NOMBRE_REPRESENTANTE'                                                         AS ultimo_nombre_del_represen_legal
    ,'SIN_IDENTIFICACION_REPRESENTANTE'                                                 AS ultimo_identific_representante_legal
    ,'SIN_SEXO_REPRESENTANTE'                                                           AS ultimo_sexo_replegal
    ,'-1'                                                                               AS id_geografia
    ,CURRENT_TIMESTAMP()                                                                AS control_timestamp_creacion

