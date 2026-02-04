{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}

select
  count(distinct receipts_id_pk) as total_orders,
  avg(fulfillment_time_min) as avg_fulfillment_time,
  sum(delivered_on_time) * 100.0 / nullif(count(delivered_on_time),0) as pct_delivered_on_time,
  sum(is_cancelled) * 100.0 / nullif(count(receipts_id_pk),0) as pct_cancelled,
  cancelled_reason,
  count(*) as cancellations_per_reason
from {{ ref('FACT_ORDERS_TIMELINES') }}
group by cancelled_reason
order by cancellations_per_reason desc
