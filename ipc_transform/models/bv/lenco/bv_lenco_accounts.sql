{{ config(materialized='table', schema='bv', tags=['Lenco']) }}

with bv_accounts as (
    select
        id                      as account_id_pk,
        name                    as account_name,
        currency                as account_currency,
        type                    as account_type,
        status                  as account_status,
        "availableBalance"      as account_available_balance_amount,
        "currentBalance"        as account_current_balance_amount,
        "bankAccount"           as account_bank_account,
        "createdAt"             as account_created_at_date_time,
        record_load_date        as account_record_load_date
    from {{ ref('raw_lenco_accounts') }}
)

select * from bv_accounts
