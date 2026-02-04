with raw_revenueledgers as (
    select * 
    from {{ source('dash_main', 'revenueledgers') }}
)

select 
    md5(cast(_id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_revenueledgers