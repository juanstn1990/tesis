SELECT 
    uuid                                                                AS uuid 
    , DT_CARGUE.id_fecha                                                AS fecha_cargue_id
    , DT_FIRMA.id_fecha                                                 AS fecha_firma_contrato_id
    , DT_INI_EJEC.id_fecha                                              AS fecha_inicio_ejec_contrato_id
    , DT_FIN_EJEC.id_fecha                                              AS fecha_fin_ejec_contrato_id
    , DT_LIQUIDACION.id_fecha                                           AS fecha_liquidacion_id
    , DT_ACTUALIZACION.id_fecha                                         AS ultima_actualizacion_id

    -- Entidad
    ,DE.id_entidad                                                       AS entidad_id
    

    -- Modalidad y Contratación
    , id_modalidad                                                      AS id_modalidad                  
    , modalidad_de_contratacion                                         AS modalidad_de_contratacion   
    , estado_del_proceso                                                AS estado_del_proceso
    , causal_de_otras_formas_de                                         AS causal_de_otras_formas_de
    , id_regimen_de_contratacion                                        AS id_regimen_de_contratacion            
    , nombre_regimen_de_contratacion                                    AS nombre_regimen_de_contratacion
    , id_objeto_a_contratar                                             AS objeto_a_contratar_id
    , tipo_de_contrato                                                  AS tipo_de_contrato  
    , cuantia_proceso                                                   AS cuantia_proceso

    -- Contratista
    , DC.id_contratista                                                 AS contratista_id

    -- Detalles del Contrato
    , plazo_de_ejec_del_contrato                                        AS plazo_de_ejec_del_contrato           
    , rango_de_ejec_del_contrato                                        AS rango_de_ejec_del_contrato       
    , tiempo_adiciones_en_dias                                          AS tiempo_adiciones_en_dias
    , tiempo_adiciones_en_meses                                         AS tiempo_adiciones_en_meses     
    , compromiso_presupuestal                                           AS compromiso_presupuestal         
    , cuantia_contrato                                                  AS cuantia_contrato               
    , valor_total_de_adiciones                                          AS valor_total_de_adiciones
    , valor_contrato_con_adiciones                                      AS valor_contrato_con_adiciones    
    , objeto_del_contrato_a_la                                          AS objeto_del_contrato_a_la

    -- Clasificación
    , DCAT.id_categoria                                                 AS categoria_id   
    , id_adjudicacion                                                   AS id_adjudicacion

    -- Evaluación
    , proponentes_seleccionados                                         AS proponentes_seleccionados
    , calificacion_definitiva                                           AS calificacion_definitiva

    -- Sub Unidad Ejecutora
    , id_sub_unidad_ejecutora                                           AS id_sub_unidad_ejecutora
    , nombre_sub_unidad_ejecutora                                       AS nombre_sub_unidad_ejecutora

    -- Otros
    , ruta_proceso_en_secop_i                                           AS ruta_proceso_en_secop_i
    , DM.id_moneda                                                      AS moneda_id
    , es_postconflicto                                                  AS es_postconflicto                          
    , marcacion_adiciones                                               AS marcacion_adiciones                 
    , posicion_rubro                                                    AS posicion_rubro                   
    , nombre_rubro                                                      AS nombre_rubro
    , valor_rubro                                                       AS valor_rubro                                 
    , pilar_acuerdo_paz                                                 AS pilar_acuerdo_paz
    , punto_acuerdo_de_paz                                              AS punto_acuerdo_de_paz
    , cumple_decreto_248                                                AS cumple_decreto_248
    , incluye_bienes_decreto_248                                        AS incluye_bienes_decreto_248
    , cumple_sentencia_t302                                             AS cumple_sentencia_t302                 
FROM 
    {{ ref('UVW_TRANS_SECOP') }} UVW
    LEFT JOIN {{ ref('DIM_ENTITY') }} DE
        ON UVW.c_digo_de_la_entidad = DE.codigo_de_la_entidad
    LEFT JOIN {{ ref('DIM_CONTRACTOR') }} DC
        ON UVW.identificacion_del_contratista = DC.identificacion_del_contratista
    LEFT JOIN {{ ref('DIM_CATEGORY') }} DCAT
        ON UVW.id_clase = DCAT.id_clase
    LEFT JOIN {{ ref('DIM_CURRENCY') }} DM
        ON UVW.moneda = DM.moneda
    LEFT JOIN {{ ref('DIM_TIME') }} DT_CARGUE
        ON UVW.fecha_cargue_secop = DT_CARGUE.fecha
    LEFT JOIN {{ ref('DIM_TIME') }} DT_FIRMA
        ON UVW.fecha_firma_contrato = DT_FIRMA.fecha
    LEFT JOIN {{ ref('DIM_TIME') }} DT_INI_EJEC
        ON UVW.fecha_inicio_ejec_contrato = DT_INI_EJEC.fecha
    LEFT JOIN {{ ref('DIM_TIME') }} DT_FIN_EJEC
        ON UVW.fecha_fin_ejec_contrato = DT_FIN_EJEC.fecha
    LEFT JOIN {{ ref('DIM_TIME') }} DT_LIQUIDACION
        ON UVW.fecha_liquidacion = DT_LIQUIDACION.fecha
    LEFT JOIN {{ ref('DIM_TIME') }} DT_ACTUALIZACION
        ON UVW.ultima_actualizacion = DT_ACTUALIZACION.fecha
