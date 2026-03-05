{{ config(materialized='table', schema='bv', tags=['Lenco'], enabled=false) }}

with bv_virtual_accounts as (
    select
        id                      as virtual_account_id_pk,
        "accountName"           as virtual_account_name,
        "accountNumber"         as virtual_account_number,
        "bankName"              as virtual_account_bank_name,
        "bankCode"              as virtual_account_bank_code,
        status                  as virtual_account_status,
        currency                as virtual_account_currency,
        "isPermanent"           as virtual_account_is_permanent,
        "trackingReference"     as virtual_account_tracking_reference,
        "createdAt"             as virtual_account_created_at_date_time,
        "updatedAt"             as virtual_account_updated_at_date_time,
        record_load_date        as virtual_account_record_load_date
    from {{ ref('raw_lenco_virtual_accounts') }}
)

select * from bv_virtual_accounts
