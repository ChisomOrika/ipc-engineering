{{ config(materialized='view') }}

with raw_inventorymovements as (
    select *
    from {{ source('gosource_main', 'inventorymovements') }}
)

select *
from raw_inventorymovements