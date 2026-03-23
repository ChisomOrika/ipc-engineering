{{ config(materialized='view') }}

with raw_requests as (
    select *
    from {{ source('gosource_main', 'requests') }}
)

select *
from raw_requests