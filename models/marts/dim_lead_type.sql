{{ config(materialized='table') }}

select 
    ROW_NUMBER() OVER(order by lead_type) as lead_type_id,
    lead_type 
from(
    select distinct lead_type
    from {{source('data_source','leads')}}
    WHERE lead_type is not NULL
)