{{ config(materialized='incremental', schema='bv', tags=['GoSource_Lenco'], unique_key='transaction_id_pk') }}

with bv_transactions as (
    select
        id                                             as transaction_id_pk,
        "accountId"                                    as transaction_account_id_fk,
        NULLIF(amount, 'NaN')::numeric                 as transaction_amount,
        type                                           as transaction_type,
        status                                         as transaction_status,
        narration                                      as transaction_narration,
        "transactionReference"                         as transaction_reference,
        "clientReference"                              as transaction_client_reference,
        NULLIF(fee, 'NaN')::numeric                    as transaction_fee_amount,
        NULLIF("initiatedAt", 'NaN')::timestamp        as transaction_initiated_at_date_time,
        NULLIF("completedAt", 'NaN')::timestamp        as transaction_completed_at_date_time,
        details                                        as transaction_details,
        record_load_date                               as transaction_record_load_date
    from {{ ref('raw_gosource_lenco_transactions') }}
    {% if is_incremental() %}
      WHERE NULLIF("initiatedAt", 'NaN')::timestamp > (SELECT MAX(transaction_initiated_at_date_time) FROM {{ this }})
    {% endif %}
)

select * from bv_transactions
