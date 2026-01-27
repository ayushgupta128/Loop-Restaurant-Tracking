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

),

limit_gem AS (

  SELECT * 
  
  FROM Union_1
  
  LIMIT 1

),

numeric_metrics_1_1 AS (

  SELECT * 
  
  FROM {{ ref('p1')}}

),

join_on_a AS (

  SELECT 
    limit_gem.a AS A,
    limit_gem.b AS B,
    limit_gem.c AS C,
    numeric_metrics_1_1.b AS N_B,
    numeric_metrics_1_1.c AS N_C
  
  FROM limit_gem
  INNER JOIN numeric_metrics_1_1
     ON limit_gem.a = numeric_metrics_1_1.a

)

SELECT *

FROM join_on_a
