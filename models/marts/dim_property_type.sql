{{ config(materialized='table') }}


SELECT DISTINCT property_type_id,
                property_type
FROM {{ source('data_source', 'sales') }}
WHERE property_type_id IS NOT NULL
