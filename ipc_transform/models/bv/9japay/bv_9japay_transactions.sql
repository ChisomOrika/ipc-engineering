{{ config(materialized='incremental', schema='bv', tags=['9japay'], unique_key='transaction_id_pk') }}

with bv_transactions as (
    select
        "transactionId"                                as transaction_id_pk,
        "accountNumber"                                as transaction_account_number,
        amount::numeric                                as transaction_amount,
        lower("transactionType")                       as transaction_type,
        narration                                      as transaction_narration,
        "transactionReference"                         as transaction_reference,
        "notificationStatus"                           as transaction_notification_status,
        metadata::text                                 as transaction_metadata,
        "transactionDate"::timestamp                   as transaction_date_time,
        record_load_date                               as transaction_record_load_date
    from {{ ref('raw_9japay_transactions') }}
    {% if is_incremental() %}
      WHERE "transactionDate"::timestamp > (SELECT MAX(transaction_date_time) FROM {{ this }})
    {% endif %}
)

select * from bv_transactions
