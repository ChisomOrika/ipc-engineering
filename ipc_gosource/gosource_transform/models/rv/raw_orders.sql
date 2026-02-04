{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_orders as (
    select * 
    from {{ source('gosource_main', 'orders') }}  -- This references the source defined in schema.yml
)

select * 
from raw_orders
