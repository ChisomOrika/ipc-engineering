{{ config(materialized='table', schema='gold', tags=['Finance', 'CashFlow']) }}

-- Unified cash flow from IPC Lenco + GoSource Lenco
-- 9japay excluded: its credits are DAASH customer collections that get swept
-- to Lenco — including both would double-count the same money.
with ipc_txns as (
    select
        transaction_id_pk,
        transaction_type,
        transaction_status,
        transaction_category,
        transaction_narration,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                      as transaction_account_id,
        transaction_completed_at_date_time::date       as transaction_date,
        'IPC'                                          as business_unit
    from {{ ref('bv_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

gosource_txns as (
    select
        transaction_id_pk,
        transaction_type,
        transaction_status,
        null::text                                     as transaction_category,
        transaction_narration,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                      as transaction_account_id,
        transaction_completed_at_date_time::date       as transaction_date,
        'GoSource'                                     as business_unit
    from {{ ref('bv_gosource_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
      and transaction_narration not like 'ACCOUNT TRANSFERS%'
      and transaction_narration not like 'From GO SOURCE%'
),

all_txns as (
    select * from ipc_txns
    union all
    select * from gosource_txns
)

select
    transaction_id_pk,
    transaction_date,
    date_trunc('month', transaction_date)::date         as transaction_month,
    date_trunc('year',  transaction_date)::date         as transaction_year,
    transaction_type,
    transaction_category,
    transaction_narration,
    transaction_reference,
    transaction_account_id,
    business_unit,

    case when transaction_type = 'credit'
         then transaction_amount else 0 end             as cash_inflow_amount,

    case when transaction_type = 'debit'
         then transaction_amount else 0 end             as cash_outflow_amount,

    case when transaction_type = 'credit'
         then transaction_amount
         else -transaction_amount end                   as net_cash_movement_amount,

    transaction_fee_amount,

    case
        when transaction_type = 'credit' then 'Inflow'
        when transaction_type = 'debit'  then 'Outflow'
    end                                                 as cash_flow_direction,

    case
        when transaction_type = 'credit'
             and lower(transaction_narration) like '%9japay%'   then '9japay Settlement'
        when transaction_type = 'credit'
             and lower(transaction_narration) like '%paystack%' then 'Paystack Settlement'
        when transaction_type = 'credit'
             and lower(transaction_narration) like '%uba%'      then 'UBA Transfer'
        when transaction_type = 'credit'
             and lower(transaction_narration) like '%inward%'   then 'Inward Transfer'
        when transaction_type = 'credit'                        then 'Other Inflow'
        else null
    end                                                 as inflow_source

from all_txns
