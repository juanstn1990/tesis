WITH date_range AS (
  SELECT 
    DATE '2005-01-01' AS start_date, 
    DATE '2100-01-01' AS end_date
),

calendar_dates AS (
  SELECT 
    calendar_date 
  FROM 
    date_range 
  CROSS JOIN 
    UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS calendar_date
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['calendar_date']) }} AS id_fecha
  ,calendar_date AS fecha
  ,EXTRACT(YEAR FROM calendar_date) AS anio
  ,EXTRACT(MONTH FROM calendar_date) AS mes
  ,FORMAT_DATE('%B', calendar_date) AS nombre_mes
  ,EXTRACT(DAY FROM calendar_date) AS dia
  ,FORMAT_DATE('%A', calendar_date) AS nombre_dia
  ,(CASE 
      WHEN FORMAT_DATE('%A', calendar_date) IN ('Sunday', 'Saturday') THEN 0 
      ELSE 1 
   END) AS dia_es_habil
  ,CURRENT_TIMESTAMP() AS control_timestamp_creacion 
FROM 
  calendar_dates

UNION ALL

SELECT 
    '-1' AS id_fecha
  ,'2999-12-31' AS fecha
  ,-1 AS anio
  ,-1 AS mes
  ,'SIN_MES' AS nombre_mes
  ,-1 AS dia
  ,'SIN_DIA' AS nombre_dia
  ,-1 AS dia_es_habil
  ,CURRENT_TIMESTAMP() AS control_timestamp_creacion
