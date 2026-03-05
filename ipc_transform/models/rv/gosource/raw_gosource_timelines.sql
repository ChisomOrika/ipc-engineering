{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_timelines as (
    select * 
    from {{ source('gosource_main', 'timelines') }}  -- This references the source defined in schema.yml
)

select * 
from raw_timelines