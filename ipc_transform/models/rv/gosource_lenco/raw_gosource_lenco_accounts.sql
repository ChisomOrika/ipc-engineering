{{ config(materialized='view') }}

with raw as (
    select *
    from {{ source('gosource_lenco_main', 'accounts') }}
)

select
    md5(cast(id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw
