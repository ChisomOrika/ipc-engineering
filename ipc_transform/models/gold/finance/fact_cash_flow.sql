{{ config(materialized='table', schema='gold', tags=['Finance', 'CashFlow']) }}

-- Daily cash flow from Lenco bank transactions only
-- 9japay excluded: its credits are DAASH customer collections that get swept
-- to Lenco — including both would double-count the same money.
-- Credits = cash inflows, Debits = cash outflows
with all_txns as (
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
        'Lenco'                                        as payment_platform
    from {{ ref('bv_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

fact as (
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
        payment_platform,

        -- Signed amount: positive = inflow, negative = outflow
        case when transaction_type = 'credit'
             then transaction_amount else 0 end             as cash_inflow_amount,

        case when transaction_type = 'debit'
             then transaction_amount else 0 end             as cash_outflow_amount,

        case when transaction_type = 'credit'
             then transaction_amount
             else -transaction_amount end                   as net_cash_movement_amount,

        transaction_fee_amount,

        -- Cash flow classification
        case
            when transaction_type = 'credit' then 'Inflow'
            when transaction_type = 'debit'  then 'Outflow'
        end                                                 as cash_flow_direction,

        -- Source classification for inflows (Lenco credits only)
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
)

select * from fact
