SELECT 
    uid                                                                             AS uuid 
    ,COALESCE(CAST(TIMESTAMP(fecha_de_cargue_en_el_secop) AS DATE),'2999-12-31')    AS fecha_cargue_secop
    ,COALESCE(CAST(TIMESTAMP(fecha_de_firma_del_contrato) AS DATE),'2999-12-31')    AS fecha_firma_contrato
    ,COALESCE(CAST(TIMESTAMP(fecha_ini_ejec_contrato) AS DATE),'2999-12-31')        AS fecha_inicio_ejec_contrato
    ,COALESCE(CAST(TIMESTAMP(fecha_fin_ejec_contrato) AS DATE),'2999-12-31')        AS fecha_fin_ejec_contrato
    ,COALESCE(CAST(TIMESTAMP(fecha_liquidacion) AS DATE),'2999-12-31')              AS fecha_liquidacion
    ,COALESCE(CAST(TIMESTAMP(ultima_actualizacion) AS DATE),'2999-12-31')           AS ultima_actualizacion

    -- Entidad

    ,COALESCE(c_digo_de_la_entidad,'SIN_CODIGO')                                    AS entidad_id

    -- Modalidad y Contratación
    , id_modalidad                                                      AS id_modalidad                  
    , {{ var('normalize_function') }}(modalidad_de_contratacion)        AS modalidad_de_contratacion   
    , {{ var('normalize_function') }}(estado_del_proceso)               AS estado_del_proceso
    , {{ var('normalize_function') }}(causal_de_otras_formas_de)        AS causal_de_otras_formas_de
    , id_regimen_de_contratacion                                        AS id_regimen_de_contratacion            
    , {{ var('normalize_function') }}(nombre_regimen_de_contratacion)
                                                                        AS nombre_regimen_de_contratacion
    , id_objeto_a_contratar
    , {{ var('normalize_function') }}(tipo_de_contrato)                 AS tipo_de_contrato  
    , cuantia_proceso                                                   AS cuantia_proceso

    -- Contratista
    , COALESCE(REGEXP_EXTRACT(SPLIT(identificacion_del_contratista, '-')[OFFSET(0)], r'\d+'),'SIN_IDENTIFICACION')
                                                                        AS identificacion_del_contratista

    -- Detalles del Contrato
    , CAST(plazo_de_ejec_del_contrato AS INT64)                         AS plazo_de_ejec_del_contrato           
    , rango_de_ejec_del_contrato                                        AS rango_de_ejec_del_contrato       
    , CAST(tiempo_adiciones_en_dias AS INT64)                           AS tiempo_adiciones_en_dias
    , CAST(tiempo_adiciones_en_meses AS INT64)                          AS tiempo_adiciones_en_meses     
    , {{ var('normalize_function') }}(compromiso_presupuestal)          AS compromiso_presupuestal         
    , CAST(cuantia_contrato AS INT64)                                   AS cuantia_contrato               
    , CAST(valor_total_de_adiciones AS INT64)                           AS valor_total_de_adiciones
    , CAST(valor_contrato_con_adiciones AS INT64)                       AS valor_contrato_con_adiciones    
    , {{ var('normalize_function') }}(objeto_del_contrato_a_la)         AS objeto_del_contrato_a_la

    -- Clasificación
    , COALESCE(id_clase,'SIN_ID_CLASE')                                 AS id_clase       
    , id_adjudicacion                                                   AS id_adjudicacion

    -- Evaluación
    , {{ var('normalize_function') }}(proponentes_seleccionados)        AS proponentes_seleccionados
    , {{ var('normalize_function') }}(calificacion_definitiva)          AS calificacion_definitiva

    -- Sub Unidad Ejecutora
    , id_sub_unidad_ejecutora                                           AS id_sub_unidad_ejecutora
    , {{ var('normalize_function') }}(nombre_sub_unidad_ejecutora)      AS nombre_sub_unidad_ejecutora

    -- Otros
    , JSON_EXTRACT_SCALAR(TO_JSON_STRING(ruta_proceso_en_secop_i), '$.url') 
                                                                        AS ruta_proceso_en_secop_i
    , COALESCE({{ var('normalize_function') }}(moneda),'SIN_MONEDA')    AS moneda
    , es_postconflicto                                                  AS es_postconflicto                          
    , marcacion_adiciones                                               AS marcacion_adiciones                 
    , {{ var('normalize_function') }}(posicion_rubro)                   AS posicion_rubro                   
    , {{ var('normalize_function') }}(nombre_rubro)                     AS nombre_rubro
    , CAST(valor_rubro AS INT64)                                        AS valor_rubro                                 
    , {{ var('normalize_function') }}(pilar_acuerdo_paz)                AS pilar_acuerdo_paz
    , {{ var('normalize_function') }}(punto_acuerdo_paz)                AS punto_acuerdo_de_paz
    , {{ var('normalize_function') }}(cumpledecreto248)                 AS cumple_decreto_248
    , {{ var('normalize_function') }}(incluyebienesdecreto248)          AS incluye_bienes_decreto_248
    , COALESCE({{ var('normalize_function') }}(cumple_sentencia_t302), 'NO DEFINIDO')
                                                                        AS cumple_sentencia_t302                 
FROM 
    {{ source('secop', 'secop') }}
