{{ config(materialized='table') }}

select ROW_NUMBER() OVER(order by method_of_contact) as method_id,
method_of_contact 
from(
    select distinct method_of_contact
    from {{source('data_source','leads')}}
    WHERE method_of_contact is not NULL
)