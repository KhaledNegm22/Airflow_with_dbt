{{ config(materialized='table') }}

select 
    ROW_NUMBER() OVER(order by campaign) as campaign_id,
    campaign 
from(
    select distinct campaign
    from {{source('data_source','leads')}}
    WHERE campaign is not NULL
) as distinct_campaign