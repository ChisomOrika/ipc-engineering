{{ config(materialized='view') }}

with raw_purchaseorders as (
    select *
    from {{ source('gosource_main', 'purchaseorders') }}
)

select *
from raw_purchaseorders