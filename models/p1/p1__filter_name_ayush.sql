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

filter_name_ayush AS (

  SELECT * 
  
  FROM entity_name_origin
  
  WHERE name = 'Ayush'

)

SELECT *

FROM filter_name_ayush
