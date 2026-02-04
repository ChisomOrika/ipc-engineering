{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_categories as (
    select * 
    from {{ source('gosource_main', 'categories') }}  -- This references the source defined in schema.yml
)

select * 
from raw_categories