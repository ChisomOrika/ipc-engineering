{{ config(materialized='table') }}

with raw_paystack_transactions as (
    select *
    from {{ source('paystack_main', 'transactions') }}
)

select
    md5(cast(id as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_paystack_transactions
