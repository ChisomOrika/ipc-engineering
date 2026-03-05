{{ config(materialized='table', schema='gold', tags=['Finance', 'CashFlow']) }}

-- Daily closing cash position derived from Lenco running balance
-- Uses the last transaction of each day as the closing balance
with lenco_txns as (
    select
        transaction_id_pk,
        transaction_account_id_fk,
        transaction_amount,
        transaction_type,
        transaction_status,
        transaction_completed_at_date_time::date    as transaction_date,
        transaction_completed_at_date_time          as transaction_datetime
    from {{ ref('bv_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

-- Daily totals
daily_flows as (
    select
        transaction_date,
        sum(case when transaction_type = 'credit' then transaction_amount else 0 end) as daily_inflow,
        sum(case when transaction_type = 'debit'  then transaction_amount else 0 end) as daily_outflow,
        sum(case when transaction_type = 'credit' then  transaction_amount
                 when transaction_type = 'debit'  then -transaction_amount
                 else 0 end)                                                           as daily_net_movement,
        count(*) as transaction_count
    from lenco_txns
    group by transaction_date
),

-- Running cumulative balance
running as (
    select
        transaction_date,
        daily_inflow,
        daily_outflow,
        daily_net_movement,
        transaction_count,
        sum(daily_net_movement) over (order by transaction_date rows unbounded preceding) as cumulative_net_movement
    from daily_flows
)

select
    transaction_date                                        as cash_position_date,
    date_trunc('month', transaction_date)::date             as cash_position_month,
    date_trunc('year',  transaction_date)::date             as cash_position_year,
    daily_inflow                                            as daily_inflow_amount,
    daily_outflow                                           as daily_outflow_amount,
    daily_net_movement                                      as daily_net_movement_amount,
    cumulative_net_movement                                 as cumulative_net_movement_amount,
    transaction_count                                       as daily_transaction_count
from running
order by transaction_date
