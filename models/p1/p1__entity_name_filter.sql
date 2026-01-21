{{
  config({    
    "materialized": "ephemeral",
    "database": "ayush_demos",
    "schema": "demos"
  })
}}

WITH entity_name_origin AS (

  SELECT * 
  
  FROM {{ source('ayush_demos_demos', 'klp') }}

),

gupta_name_filter AS (

  SELECT * 
  
  FROM entity_name_origin AS in0
  
  WHERE name = 'Gupta'

),

entity_name_filter AS (

  SELECT 
    name,
    row_origin
  
  FROM gupta_name_filter AS in0

)

SELECT *

FROM entity_name_filter
