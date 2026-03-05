{{ config(materialized='table', schema='bv', tags=['Lenco'], enabled=false) }}

with bv_pos_transactions as (
    select
        id                      as pos_transaction_id_pk,
        "terminalId"            as pos_transaction_terminal_id_fk,
        amount                  as pos_transaction_amount,
        currency                as pos_transaction_currency,
        status                  as pos_transaction_status,
        type                    as pos_transaction_type,
        narration               as pos_transaction_narration,
        reference               as pos_transaction_reference,
        rrn                     as pos_transaction_rrn,
        datetime                as pos_transaction_date_time,
        record_load_date        as pos_transaction_record_load_date
    from {{ ref('raw_lenco_pos_transactions') }}
)

select * from bv_pos_transactions
