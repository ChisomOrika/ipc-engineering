{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_customers as (
    select * 
    from {{ source('gosource_main', 'customers') }}  -- This references the source defined in schema.yml
)

select * 
from raw_customers