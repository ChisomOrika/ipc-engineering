{{ config(materialized='table', schema='bv', tags=['Lenco'], enabled=false) }}

with bv_virtual_account_transactions as (
    select
        id                      as va_transaction_id_pk,
        "accountReference"      as va_transaction_account_reference,
        amount                  as va_transaction_amount,
        currency                as va_transaction_currency,
        status                  as va_transaction_status,
        type                    as va_transaction_type,
        narration               as va_transaction_narration,
        reference               as va_transaction_reference,
        "sessionId"             as va_transaction_session_id,
        "settlementId"          as va_transaction_settlement_id,
        datetime                as va_transaction_date_time,
        record_load_date        as va_transaction_record_load_date
    from {{ ref('raw_lenco_virtual_account_transactions') }}
)

select * from bv_virtual_account_transactions
