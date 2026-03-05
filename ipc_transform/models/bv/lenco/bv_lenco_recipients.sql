{{ config(materialized='table', schema='bv', tags=['Lenco']) }}

with bv_recipients as (
    select
        id                                          as recipient_id_pk,
        name                                        as recipient_name,
        currency                                    as recipient_currency,
        "bankAccount"::jsonb->>'accountName'          as recipient_account_name,
        "bankAccount"::jsonb->>'accountNumber'        as recipient_account_number,
        "bankAccount"::jsonb->>'bankCode'             as recipient_bank_code,
        "bankAccount"::jsonb->>'bankName'             as recipient_bank_name,
        "bankAccount"::jsonb->>'type'                 as recipient_type,
        NULL::timestamp                             as recipient_created_at_date_time,
        NULL::timestamp                             as recipient_updated_at_date_time,
        record_load_date                            as recipient_record_load_date
    from {{ ref('raw_lenco_recipients') }}
)

select * from bv_recipients
