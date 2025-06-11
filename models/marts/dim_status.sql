{{ config(materialized='table') }}

select 
    ROW_NUMBER() OVER(ORDER by status_name) as status_id,
    status_name
from (
    select DISTINCT status_name 
    from {{ source('data_source','leads') }}
    WHERE status_name is not NULL
) as distinct_status