{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}

with order_statuses as (
  select
    order_id_fk,
    min(case when timelines_title = 'accepted' then timelines_createdat_date_time end) as accepted_createdAt,
    max(case when timelines_title = 'delivered' then timelines_createdat_date_time end) as delivered_createdAt,
    max(case when timelines_title = 'cancelled' then timelines_createdat_date_time end) as cancelled_createdAt,
    max(case when timelines_title = 'cancelled' then timelines_description end) as cancelled_reason,
    min(timelines_createdat_date_time) as order_createdAt
  from {{ ref('bv_timelines') }}
  group by order_id_fk
),

order_metrics as (
  select
    o.receipts_id_pk,
    o.receipts_createdat_date,
    s.accepted_createdAt,
    s.delivered_createdAt,
    s.cancelled_createdAt,
    s.cancelled_reason,

    -- Fulfillment time in minutes
    CASE
      WHEN o.receipts_createdat_date IS NOT NULL AND s.delivered_createdAt IS NOT NULL THEN
        EXTRACT(EPOCH FROM (s.delivered_createdAt - o.receipts_createdat_date)) / 60
      ELSE NULL
    END as fulfillment_time_min,


    -- Flag for cancellation
    CASE
      WHEN s.cancelled_createdAt IS NOT NULL THEN 1
      ELSE 0
    END as is_cancelled

  from {{ ref('bv_receipts') }} o
  left join order_statuses s on o.receipts_id_pk = s.order_id_fk
)

select
  receipts_id_pk,
  receipts_createdat_date,
  accepted_createdAt,
  delivered_createdAt,
  cancelled_createdAt,
  cancelled_reason,
  fulfillment_time_min,
  CASE
    WHEN fulfillment_time_min IS NOT NULL AND fulfillment_time_min <= 1440 THEN 1
    ELSE 0
  END as delivered_on_time,
  is_cancelled
from order_metrics
