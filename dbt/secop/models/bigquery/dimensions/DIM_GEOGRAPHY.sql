WITH GEOGRAFIA AS (
  SELECT DISTINCT 
    {{ var('normalize_function') }}(municipio_entidad)              AS ciudad
    ,{{ var('normalize_function') }}(departamento_entidad)          AS departamento
    ,'CIUDAD'                                                       AS tipo_geografia
  FROM
    {{ source('secop', 'secop') }}
  WHERE municipio_entidad IS NOT NULL AND departamento_entidad IS NOT NULL
)
SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['ciudad', 'departamento']) }}AS id_geografia
  ,ciudad                                                             AS ciudad
  ,departamento                                                       AS departamento
  ,tipo_geografia                                                     AS tipo_geografia
  ,CASE
      WHEN departamento IN ('ARAUCA', 'META', 'GUAVIARE', 'VICHADA', 'CASANARE', 'VAUPES') THEN 'Orinoquía'
      WHEN departamento IN ('CAUCA', 'CHOCO', 'NARINO', 'VALLE DEL CAUCA', 'RISARALDA', 'QUINDIO', 'CALDAS', 'TOLIMA', 'HUILA') THEN 'Pacífico'
      WHEN departamento IN ('CESAR', 'SUCRE', 'MAGDALENA', 'ATLANTICO', 'BOLIVAR', 'CORDOBA', 'SAN ANDRES, PROVIDENCIA Y SANTA CATALINA', 'LA GUAJIRA') THEN 'Caribe'
      WHEN departamento IN ('BOGOTA D.C.', 'ANTIOQUIA', 'BOYACA', 'CUNDINAMARCA', 'NORTE DE SANTANDER', 'SANTANDER') THEN 'Andina'
      WHEN departamento IN ('GUAINIA', 'AMAZONAS', 'PUTUMAYO', 'CAQUETA') THEN 'Amazonía'
      WHEN departamento = 'COLOMBIA' THEN 'Nacional'
      ELSE 'Desconocida'
  END                                                                 AS region
  ,CURRENT_TIMESTAMP()                                                AS control_timestamp_creacion 
FROM
  GEOGRAFIA
WHERE 
    ciudad != 'NO DEFINIDO' 
    AND departamento != 'NO DEFINIDO'

UNION ALL

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['-1', 'departamento']) }}   AS id_geografia
  ,'SIN_CIUDAD'                                                      AS ciudad
  ,departamento                                                      AS departamento
  ,'DEPARTAMENTO'                                                    AS tipo_geografia
  ,CASE
      WHEN departamento IN ('ARAUCA', 'META', 'GUAVIARE', 'VICHADA', 'CASANARE', 'VAUPES') THEN 'Orinoquía'
      WHEN departamento IN ('CAUCA', 'CHOCO', 'NARINO', 'VALLE DEL CAUCA', 'RISARALDA', 'QUINDIO', 'CALDAS', 'TOLIMA', 'HUILA') THEN 'Pacífico'
      WHEN departamento IN ('CESAR', 'SUCRE', 'MAGDALENA', 'ATLANTICO', 'BOLIVAR', 'CORDOBA', 'SAN ANDRES, PROVIDENCIA Y SANTA CATALINA', 'LA GUAJIRA') THEN 'Caribe'
      WHEN departamento IN ('BOGOTA D.C.', 'ANTIOQUIA', 'BOYACA', 'CUNDINAMARCA', 'NORTE DE SANTANDER', 'SANTANDER') THEN 'Andina'
      WHEN departamento IN ('GUAINIA', 'AMAZONAS', 'PUTUMAYO', 'CAQUETA') THEN 'Amazonía'
      WHEN departamento = 'COLOMBIA' THEN 'Nacional'
      ELSE 'Desconocida'
  END                                                                 AS region
  ,CURRENT_TIMESTAMP()                                                AS control_timestamp_creacion 
FROM
  GEOGRAFIA
WHERE 
    departamento != 'NO DEFINIDO'

UNION ALL

SELECT 
    '-1'                                                             AS id_geografia
  ,'SIN_CIUDAD'                                                      AS ciudad
  ,'SIN_DEPARTAMENTO'                                                AS departamento
  ,'SIN_TIPO_GEOGRAFIA'                                              AS tipo_geografia
  ,'SIN_REGION'                                                      AS region
  ,CURRENT_TIMESTAMP()                                               AS control_timestamp_creacion 
