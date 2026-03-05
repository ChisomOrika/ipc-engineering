{{ config(materialized='table', schema='gold', tags=['Finance', 'Expenses']) }}

-- Operational expense breakdown from Lenco debit transactions
with lenco_debits as (
    select
        transaction_id_pk,
        transaction_narration,
        transaction_category,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk,
        transaction_completed_at_date_time::date    as transaction_date
    from {{ ref('bv_lenco_transactions') }}
    where transaction_type   = 'debit'
      and transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
)

select
    transaction_id_pk                                               as expense_id_pk,
    transaction_date                                                as expense_date,
    date_trunc('month', transaction_date)::date                     as expense_month,
    date_trunc('year',  transaction_date)::date                     as expense_year,
    transaction_category                                            as expense_category,

    -- High-level grouping for dashboard
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

    -- Fixed vs variable indicator
    case
        when transaction_category in ('Salaries', 'Utilities', 'Data & Calls',
                                       'Docs & Compliance', 'Repairs & Maintenance')
             then 'Fixed'
        else 'Variable'
    end                                                             as expense_type,

    transaction_narration                                           as expense_narration,
    transaction_reference                                           as expense_reference,
    transaction_account_id_fk                                      as expense_account_id_fk,
    transaction_amount                                              as expense_amount,
    transaction_fee_amount                                          as expense_fee_amount

from lenco_debits
