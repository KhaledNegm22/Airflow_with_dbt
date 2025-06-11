{{ config(materialized='table') }}


SELECT 
    ROW_NUMBER() OVER (ORDER BY unit_location) AS unit_location_id,
    unit_location
FROM (
    SELECT DISTINCT unit_location
    FROM {{ source('data_source', 'sales') }}
    WHERE unit_location IS NOT NULL
) 
