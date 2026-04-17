{{ config(materialized='table', schema='gold', tags=['Finance', 'Expenses']) }}

-- Unified expenses from IPC Lenco + GoSource Lenco debit transactions
-- 9japay debits excluded: they are internal sweep transfers, not real expenses
with ipc_debits as (
    select
        transaction_id_pk,
        transaction_narration,
        transaction_category,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                      as transaction_account_id,
        transaction_completed_at_date_time::date       as transaction_date,
        'IPC'                                          as business_unit
    from {{ ref('bv_lenco_transactions') }}
    where transaction_type   = 'debit'
      and transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

gosource_debits as (
    select
        transaction_id_pk,
        transaction_narration,
        null::text                                     as transaction_category,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                      as transaction_account_id,
        transaction_completed_at_date_time::date       as transaction_date,
        'GoSource'                                     as business_unit
    from {{ ref('bv_gosource_lenco_transactions') }}
    where transaction_type   = 'debit'
      and transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

all_debits as (
    select * from ipc_debits
    union all
    select * from gosource_debits
)

select
    transaction_id_pk                                               as expense_id_pk,
    transaction_date                                                as expense_date,
    date_trunc('month', transaction_date)::date                     as expense_month,
    date_trunc('year',  transaction_date)::date                     as expense_year,
    transaction_category                                            as expense_category,
    business_unit,

    case
        when transaction_category in ('Salaries', 'Staff Welfare', 'Rider Commission')
             then 'People & Payroll'
        when transaction_category in ('Supplies & Procurement')
             then 'Supplies & Procurement'
        when transaction_category in ('Fuel & Diesel', 'Transport')
             then 'Logistics & Fuel'
        when transaction_category in ('Repairs & Maintenance', 'Utilities')
             then 'Operations & Maintenance'
        when transaction_category in ('Marketing')
             then 'Marketing'
        when transaction_category in ('Data & Calls', 'Docs & Compliance')
             then 'Admin & Compliance'
        when transaction_category in ('Bank Charges', 'Inward Transfer')
             then 'Bank & Finance'
        else 'Uncategorized'
    end                                                             as expense_group,

    case
        when transaction_category in ('Salaries', 'Utilities', 'Data & Calls',
                                       'Docs & Compliance', 'Repairs & Maintenance')
             then 'Fixed'
        else 'Variable'
    end                                                             as expense_type,

    transaction_narration                                           as expense_narration,
    transaction_reference                                           as expense_reference,
    transaction_account_id                                          as expense_account_id,
    transaction_amount                                              as expense_amount,
    transaction_fee_amount                                          as expense_fee_amount

from all_debits
