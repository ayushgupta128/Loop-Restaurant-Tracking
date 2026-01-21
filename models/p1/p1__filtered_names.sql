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

filtered_names AS (

  SELECT 
    name,
    row_origin
  
  FROM entity_name_origin

)

SELECT *

FROM filtered_names
