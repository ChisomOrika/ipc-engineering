{{ config(materialized='table', enabled=false) }}

with raw_lenco_virtual_accounts as (
    select *
    from {{ source('lenco_main', 'virtual_accounts') }}
)

select
    md5(cast(id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_lenco_virtual_accounts
