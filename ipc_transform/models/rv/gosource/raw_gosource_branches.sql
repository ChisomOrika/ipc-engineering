{{ config(materialized='view') }}

with raw_branches as (
    select *
    from {{ source('gosource_main', 'branches') }}
)

select *
from raw_branches