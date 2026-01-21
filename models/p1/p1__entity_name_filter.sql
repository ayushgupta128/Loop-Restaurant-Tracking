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

entity_name_filter AS (

  {#Finds records for individuals named Ayush or Gupta.#}
  SELECT * 
  
  FROM entity_name_origin AS in0
  
  WHERE name = 'Ayush' OR name = 'Gupta'

)

SELECT *

FROM entity_name_filter
