with raw_wallets as (
    select * 
    from {{ source('dash_main', 'wallets') }}
)

select 
    md5(cast(_id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_wallets










