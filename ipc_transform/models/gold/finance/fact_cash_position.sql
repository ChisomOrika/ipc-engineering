{{ config(materialized='table', schema='gold', tags=['Finance', 'CashFlow']) }}

-- Daily closing cash position per business unit from Lenco running balance
with ipc_txns as (
    select
        transaction_amount,
        transaction_type,
        transaction_completed_at_date_time::date    as transaction_date,
        'IPC'                                       as business_unit
    from {{ ref('bv_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

gosource_txns as (
    select
        transaction_amount,
        transaction_type,
        transaction_completed_at_date_time::date    as transaction_date,
        'GoSource'                                  as business_unit
    from {{ ref('bv_gosource_lenco_transactions') }}
    where transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

all_txns as (
    select * from ipc_txns
    union all
    select * from gosource_txns
),

daily_flows as (
    select
        transaction_date,
        business_unit,
        sum(case when transaction_type = 'credit' then transaction_amount else 0 end) as daily_inflow,
        sum(case when transaction_type = 'debit'  then transaction_amount else 0 end) as daily_outflow,
        sum(case when transaction_type = 'credit' then  transaction_amount
                 when transaction_type = 'debit'  then -transaction_amount
                 else 0 end)                                                           as daily_net_movement,
        count(*) as transaction_count
    from all_txns
    group by transaction_date, business_unit
),

running as (
    select
        transaction_date,
        business_unit,
        daily_inflow,
        daily_outflow,
        daily_net_movement,
        transaction_count,
        sum(daily_net_movement) over (
            partition by business_unit
            order by transaction_date
            rows unbounded preceding
        ) as cumulative_net_movement
    from daily_flows
)

select
    transaction_date                                        as cash_position_date,
    date_trunc('month', transaction_date)::date             as cash_position_month,
    date_trunc('year',  transaction_date)::date             as cash_position_year,
    business_unit,
    daily_inflow                                            as daily_inflow_amount,
    daily_outflow                                           as daily_outflow_amount,
    daily_net_movement                                      as daily_net_movement_amount,
    cumulative_net_movement                                 as cumulative_net_movement_amount,
    transaction_count                                       as daily_transaction_count
from running
order by transaction_date, business_unit
