{{ config(materialized='table', enabled=false) }}

with raw_lenco_pos_terminals as (
    select *
    from {{ source('lenco_main', 'pos_terminals') }}
)

select
    md5(cast(id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_lenco_pos_terminals
