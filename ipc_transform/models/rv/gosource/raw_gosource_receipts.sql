{{ config(materialized='table') }}  -- This can be adjusted as needed (table, view, etc.)

with raw_receipts as (
    select * 
    from {{ source('gosource_main', 'receipts') }}  -- This references the source defined in schema.yml
)

select * 
from raw_receipts