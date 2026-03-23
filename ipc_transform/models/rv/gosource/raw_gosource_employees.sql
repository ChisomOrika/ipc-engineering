{{ config(materialized='view') }}

with raw_employees as (
    select *
    from {{ source('gosource_main', 'employees') }}
)

select *
from raw_employees