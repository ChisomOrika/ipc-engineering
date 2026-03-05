{{ config(materialized='table', schema='bv', tags=['Lenco'], enabled=false) }}

with bv_bills as (
    select
        id                      as bill_id_pk,
        category                as bill_category,
        type                    as bill_type,
        amount                  as bill_amount,
        currency                as bill_currency,
        status                  as bill_status,
        narration               as bill_narration,
        reference               as bill_reference,
        "phoneNumber"           as bill_phone_number,
        provider                as bill_provider,
        "initiatedAt"           as bill_initiated_at_date_time,
        "completedAt"           as bill_completed_at_date_time,
        record_load_date        as bill_record_load_date
    from {{ ref('raw_lenco_bills') }}
)

select * from bv_bills
