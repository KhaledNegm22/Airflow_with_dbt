{{ config(materialized='table') }}


SELECT 
    ROW_NUMBER() OVER (ORDER BY sale_category) AS sale_category_id,
    sale_category
FROM (
    SELECT DISTINCT sale_category
    FROM {{ source('data_source', 'sales') }}
    WHERE sale_category IS NOT NULL
)


