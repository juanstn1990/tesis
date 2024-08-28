WITH CONTRATISTAS AS (
    SELECT 
        tipo_identifi_del_contratista AS tipo_identifi_del_contratista,
        REGEXP_EXTRACT(SPLIT(COALESCE(identificacion_del_contratista, ''), '-')[OFFSET(0)], r'\d+') AS identificacion_del_contratista,
        COALESCE(nom_razon_social_contratista, 'SIN_NOMBRE_CONTRATISTA') AS nom_razon_social_contratista,
        COALESCE(dpto_y_muni_contratista, 'SIN_DEPARTAMENTO') AS dpto_y_muni_contratista,
        COUNT(*) AS conteo_
    FROM
        {{ source('secop', 'secop') }}
    GROUP BY 1, 2, 3, 4
),
ULTIMO_REPRESENTANTE AS (
    SELECT 
        REGEXP_EXTRACT(SPLIT(COALESCE(identificacion_del_contratista, ''), '-')[OFFSET(0)], r'\d+') AS identificacion_del_contratista_mod,
        COALESCE(tipo_doc_representante_legal, 'SIN_TIPO_DOC_REPRESENTANTE') AS tipo_doc_representante_legal,
        COALESCE(nombre_del_represen_legal, 'SIN_NOMBRE_REPRESENTANTE') AS nombre_del_represen_legal,
        COALESCE(identific_representante_legal, 'SIN_IDENTIFICACION_REPRESENTANTE') AS identific_representante_legal,
        COALESCE(sexo_replegal, 'SIN_SEXO_REPRESENTANTE') AS sexo_replegal,
        CAST(TIMESTAMP(fecha_de_cargue_en_el_secop) AS DATE) AS fecha_cargue
    FROM 
        `artful-sled-419501`.secop.secop
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY identificacion_del_contratista_mod ORDER BY fecha_cargue DESC) = 1
),
CONTRATISTAS_MOD AS (
    SELECT
        tipo_identifi_del_contratista AS tipo_identifi_del_contratista,
        identificacion_del_contratista AS identificacion_del_contratista,
        COALESCE(nom_razon_social_contratista, 'SIN_NOMBRE_CONTRATISTA') AS nom_razon_social_contratista,
        COALESCE(dpto_y_muni_contratista, '-1') AS dpto_y_muni_contratista
    FROM 
        CONTRATISTAS
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY identificacion_del_contratista ORDER BY conteo_ DESC) = 1
),
CONTRATISTAS_SECOP2 AS (
    SELECT 
        REGEXP_EXTRACT(SPLIT(COALESCE(nit, ''), '-')[OFFSET(0)], r'\d+') AS nit,
        {{ var('normalize_function') }}(COALESCE(nombre, 'SIN_NOMBRE_CONTRATISTA')) AS nom_razon_social_contratista,
        CASE 
            WHEN SEC2.departamento = 'Distrito Capital de Bogotá' THEN 'BOGOTA D.C.'
            WHEN SEC2.departamento = 'No Definido' THEN 'SIN_DEPARTAMENTO'
            ELSE COALESCE(SEC2.departamento, 'SIN_DEPARTAMENTO')
        END AS dpto_y_muni_contratista,
        {{ var('normalize_function') }}(COALESCE(tipo_doc_representante_legal, 'SIN_TIPO_DOC_REPRESENTANTE')) AS ultimo_tipo_doc_representante_legal,
        {{ var('normalize_function') }}(COALESCE(nombre_representante_legal, 'SIN_NOMBRE_REPRESENTANTE')) AS ultimo_nombre_del_represen_legal,
        COALESCE(num_doc_representante_legal, 'SIN_IDENTIFICACION_REPRESENTANTE') AS ultimo_identific_representante_legal,
        'SIN_SEXO_REPRESENTANTE' AS ultimo_sexo_replegal                                                                                     
    FROM 
        `artful-sled-419501`.secop.proveedores_secop2 SEC2
),
ALL_CONTRACTORS AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['identificacion_del_contratista']) }} AS id_contratista,
        {{ var('normalize_function') }}(COALESCE(tipo_identifi_del_contratista, 'SIN_IDENTIFICACION')) AS tipo_identifi_del_contratista,
        identificacion_del_contratista AS identificacion_del_contratista,
        {{ var('normalize_function') }}(COALESCE(nom_razon_social_contratista, 'SIN_NOMBRE_CONTRATISTA')) AS nom_razon_social_contratista,
        COALESCE(DG1.departamento, 'SIN_DEPARTAMENTO') AS dpto_y_muni_contratista,
        {{ var('normalize_function') }}(COALESCE(UR.tipo_doc_representante_legal, 'SIN_TIPO_DOC_REPRESENTANTE')) AS ultimo_tipo_doc_representante_legal,
        {{ var('normalize_function') }}(COALESCE(UR.nombre_del_represen_legal, 'SIN_NOMBRE_REPRESENTANTE')) AS ultimo_nombre_del_represen_legal,
        {{ var('normalize_function') }}(COALESCE(UR.identific_representante_legal, 'SIN_IDENTIFICACION_REPRESENTANTE')) AS ultimo_identific_representante_legal,
        {{ var('normalize_function') }}(COALESCE(UR.sexo_replegal, 'SIN_SEXO_REPRESENTANTE')) AS ultimo_sexo_replegal,
        COALESCE(DG1.id_geografia, '-1') AS id_geografia,
        CURRENT_TIMESTAMP() AS control_timestamp_creacion,
        1 AS source,
        'secop 1' AS source_name                                                                                       
    FROM 
        CONTRATISTAS_MOD
    LEFT JOIN 
        {{ ref('DIM_GEOGRAPHY') }} DG1
        ON {{ var('normalize_function') }}(COALESCE(dpto_y_muni_contratista, 'SIN_DEPARTAMENTO')) = DG1.departamento
        AND DG1.tipo_geografia = 'DEPARTAMENTO'
    LEFT JOIN 
        ULTIMO_REPRESENTANTE UR
        ON CONTRATISTAS_MOD.identificacion_del_contratista = UR.identificacion_del_contratista_mod
    WHERE
        identificacion_del_contratista IS NOT NULL

    UNION ALL

    SELECT 
        '-1' AS id_contratista,
        'SIN_TIPO_IDENTIFICACION' AS tipo_identifi_del_contratista,
        'SIN_IDENTIFICACION' AS identificacion_del_contratista,
        'SIN_NOMBRE_CONTRATISTA' AS nom_razon_social_contratista,
        'SIN_DEPARTAMENTO' AS dpto_y_muni_contratista,
        'SIN_TIPO_DOC_REPRESENTANTE' AS ultimo_tipo_doc_representante_legal,
        'SIN_NOMBRE_REPRESENTANTE' AS ultimo_nombre_del_represen_legal,
        'SIN_IDENTIFICACION_REPRESENTANTE' AS ultimo_identific_representante_legal,
        'SIN_SEXO_REPRESENTANTE' AS ultimo_sexo_replegal,
        '-1' AS id_geografia,
        CURRENT_TIMESTAMP() AS control_timestamp_creacion,
        1 AS source,
        'default' AS source_name   

    UNION ALL

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['nit']) }} AS id_contratista,
        'SIN_TIPO_IDENTIFICACION' AS tipo_identifi_del_contratista,
        nit AS identificacion_del_contratista,
        {{ var('normalize_function') }}(COALESCE(nom_razon_social_contratista, 'SIN_NOMBRE_CONTRATISTA')) AS nom_razon_social_contratista,
        COALESCE(DG1.departamento, 'SIN_DEPARTAMENTO') AS dpto_y_muni_contratista,
        'na' AS ultimo_tipo_doc_representante_legal,
        COALESCE(ultimo_nombre_del_represen_legal, 'SIN_NOMBRE_REPRESENTANTE') AS ultimo_nombre_del_represen_legal,
        COALESCE(ultimo_identific_representante_legal, 'SIN_IDENTIFICACION_REPRESENTANTE') AS ultimo_identific_representante_legal,
        COALESCE(ultimo_sexo_replegal, 'SIN_SEXO_REPRESENTANTE') AS ultimo_sexo_replegal,
        COALESCE(DG1.id_geografia, '-1') AS id_geografia,
        CURRENT_TIMESTAMP() AS control_timestamp_creacion,
        2 AS source,
        'secop 2' AS source_name   
    FROM
        CONTRATISTAS_SECOP2 AC
    LEFT JOIN 
        {{ ref('DIM_GEOGRAPHY') }} DG1
        ON {{ var('normalize_function') }}(COALESCE(dpto_y_muni_contratista, '-1')) = DG1.departamento
        AND DG1.tipo_geografia = 'DEPARTAMENTO'
    WHERE
        AC.nit IS NOT NULL
)
SELECT 
    * 
FROM 
    ALL_CONTRACTORS
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY identificacion_del_contratista ORDER BY source ASC) = 1
