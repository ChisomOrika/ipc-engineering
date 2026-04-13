{{ config(materialized='table', schema='gold', tags=['Finance', 'CashFlow']) }}

-- Daily cash flow from Lenco bank + 9japay virtual account transactions
-- Credits = cash inflows, Debits = cash outflows
with lenco_txns as (
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

nine_japay_txns as (
    select
        transaction_id_pk,
        transaction_type,
        null::text                                     as transaction_status,
        null::text                                     as transaction_category,
        transaction_narration,
        transaction_amount,
        null::numeric                                  as transaction_fee_amount,
        transaction_reference,
        transaction_account_number                     as transaction_account_id,
        transaction_date_time::date                    as transaction_date,
        '9japay'                                       as payment_platform
    from {{ ref('bv_9japay_transactions') }}
    where transaction_date_time is not null
),

all_txns as (
    select * from lenco_txns
    union all
    select * from nine_japay_txns
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

        -- Source classification for inflows
        case
            when transaction_type = 'credit'
                 and payment_platform = '9japay'                    then '9japay Customer Payment'
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
