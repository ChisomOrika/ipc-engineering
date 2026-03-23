{{ config(materialized='view') }}

with raw_employeeinvites as (
    select *
    from {{ source('gosource_main', 'employeeinvites') }}
)

select *
from raw_employeeinvites