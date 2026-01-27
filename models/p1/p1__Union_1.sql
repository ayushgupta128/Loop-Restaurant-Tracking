{{
  config({    
    "materialized": "ephemeral",
    "database": "ayush_demos",
    "schema": "demos"
  })
}}

WITH numeric_metrics_1 AS (

  SELECT * 
  
  FROM {{ ref('p1')}}

),

numeric_metrics AS (

  SELECT * 
  
  FROM {{ ref('p1')}}

),

Union_1 AS (

  SELECT * 
  
  FROM numeric_metrics AS in0
  
  UNION
  
  SELECT * 
  
  FROM numeric_metrics_1 AS in1

)

SELECT *

FROM Union_1
