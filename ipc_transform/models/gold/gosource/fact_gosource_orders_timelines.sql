{{ config(
    materialized='incremental',
    schema='gold',
    tags=['GoSource'],
    unique_key='receipts_id_pk'
) }}

with order_statuses as (
  select
    order_id_fk,
    min(case when timelines_title = 'accepted' then timelines_created_at_date_time end) as accepted_created_at,
    max(case when timelines_title = 'delivered' then timelines_created_at_date_time end) as delivered_created_at,
    max(case when timelines_title = 'cancelled' then timelines_created_at_date_time end) as cancelled_created_at,
    max(case when timelines_title = 'cancelled' then timelines_description end) as cancelled_reason,
    min(timelines_created_at_date_time) as order_created_at
  from {{ ref('bv_gosource_timelines') }}
  group by order_id_fk
),

order_metrics as (
  select
    o.receipts_id_pk,
    o.receipts_created_at_date,
    o.receipts_updated_at_date,
    s.accepted_created_at,
    s.delivered_created_at,
    s.cancelled_created_at,
    s.cancelled_reason,

    -- Fulfillment time in minutes
    CASE
      WHEN o.receipts_created_at_date IS NOT NULL AND s.delivered_created_at IS NOT NULL THEN
        EXTRACT(EPOCH FROM (s.delivered_created_at - o.receipts_created_at_date)) / 60
      ELSE NULL
    END as fulfillment_time_min,

    -- Flag for cancellation
    CASE
      WHEN s.cancelled_created_at IS NOT NULL THEN 1
      ELSE 0
    END as is_cancelled

  from {{ ref('bv_gosource_receipts') }} o
  left join order_statuses s on o.receipts_id_pk = s.order_id_fk

  {% if is_incremental() %}
    WHERE o.receipts_updated_at_date > (SELECT MAX(receipts_updated_at_date) FROM {{ this }})
  {% endif %}
)

select
  receipts_id_pk,
  receipts_created_at_date,
  receipts_updated_at_date,
  accepted_created_at,
  delivered_created_at,
  cancelled_created_at,
  cancelled_reason,
  fulfillment_time_min,
  CASE
    WHEN fulfillment_time_min IS NOT NULL AND fulfillment_time_min <= 1440 THEN 1
    ELSE 0
  END as delivered_on_time,
  is_cancelled
from order_metrics
