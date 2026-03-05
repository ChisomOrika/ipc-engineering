{{ config(materialized='incremental', schema='bv', tags=['Lenco'], unique_key='transaction_id_pk') }}

with bv_transactions as (
    select
        id                              as transaction_id_pk,
        "accountId"                     as transaction_account_id_fk,
        NULLIF(amount, 'NaN')::numeric             as transaction_amount,
        NULL::text                                 as transaction_currency,
        type                                       as transaction_type,
        status                                     as transaction_status,
        narration                                  as transaction_narration,
        category                                   as transaction_category,
        "transactionReference"                     as transaction_reference,
        NULLIF(fee, 'NaN')::numeric                as transaction_fee_amount,
        NULL::numeric                              as transaction_running_balance_amount,
        NULLIF("initiatedAt", 'NaN')::timestamp    as transaction_initiated_at_date_time,
        NULLIF("completedAt", 'NaN')::timestamp    as transaction_completed_at_date_time,
        NULL::timestamp                 as transaction_created_at_date_time,
        NULL::timestamp                 as transaction_updated_at_date_time,
        record_load_date                as transaction_record_load_date
    from {{ ref('raw_lenco_transactions') }}
    {% if is_incremental() %}
      WHERE NULLIF("initiatedAt", 'NaN')::timestamp > (SELECT MAX(transaction_initiated_at_date_time) FROM {{ this }})
    {% endif %}
)

select * from bv_transactions
