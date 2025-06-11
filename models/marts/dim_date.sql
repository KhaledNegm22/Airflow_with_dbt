{{ config(materialized='table') }}

with date_series as (
    select generate_series(
        date '2016-12-01',  
        CURRENT_DATE, 
        interval '1 day'
    )::date as full_date
)

select 
    to_char(full_date, 'YYYYMMDD')::int as date_id,
    extract(year from full_date)::int as year,
    extract(quarter from full_date)::int as quarter,
    extract(month from full_date)::int as month,
    extract(week from full_date)::int as week,
    extract(day from full_date)::int as day
from date_series
order by full_date
