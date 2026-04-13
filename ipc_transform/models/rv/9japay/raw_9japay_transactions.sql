{{ config(materialized='view') }}

with raw_9japay_transactions as (
    select *
    from {{ source('9japay_main', 'transactions') }}
)

select
    md5(cast("transactionId" as text)) as id_hash_key,
    *,
    current_timestamp as record_load_date
from raw_9japay_transactions
