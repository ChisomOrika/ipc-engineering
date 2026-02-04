{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_products as (
    select * 
    from {{ source('gosource_main', 'products') }}  -- This references the source defined in schema.yml
)

select * 
from raw_products