{{ config(materialized='table') }}

select 
    ROW_NUMBER() OVER(order by lead_source) as lead_source_id,
    lead_source 
from(
    select distinct lead_source
    from {{source('data_source','leads')}}
    WHERE lead_source is not NULL
)