{{ config(materialized='view') }}

with raw_wallets as (
    select *
    from {{ source('gosource_main', 'wallets') }}
)

select *
from raw_wallets