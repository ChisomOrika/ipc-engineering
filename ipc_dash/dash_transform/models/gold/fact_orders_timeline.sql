{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH orders_timelines AS (

SELECT
  o.orders_id_pk AS order_id,
  o.orders_customer_id_fk AS customer_id,
  c.customer_business_name AS customer_name,
  o.orders_branch_id_fk AS branch_id,
  b.branches_name AS branch_name,
  o.orders_user AS user_id,
  o.orders_order_type,
  o.orders_delivery_type,
  o.orders_status,
  o.orders_channel,
  o.orders_duration,
  o.orders_created_at_date_time AS order_created_at,
  o.orders_updated_at_date_time AS order_updated_at,

  -- Extract timeline timestamps
  (
    SELECT (elem ->> 'createdAt')::timestamp
    FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'accepted'
    LIMIT 1
  ) AS accepted_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'preparing'
    LIMIT 1
  ) AS preparing_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'ready'
    LIMIT 1
  ) AS ready_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'shipped'
    LIMIT 1
  ) AS shipped_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'delivered'
    LIMIT 1
  ) AS delivered_at,

  -- Calculate durations in minutes (NULL if any timestamp missing)
  CASE 
    WHEN (
      SELECT (elem ->> 'createdAt')::timestamp
      FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
      WHERE elem ->> 'title' = 'accepted'
      LIMIT 1
    ) IS NOT NULL
    AND (
      SELECT (elem ->> 'updatedAt')::timestamp
      FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
      WHERE elem ->> 'title' = 'delivered'
      LIMIT 1
    ) IS NOT NULL
    THEN EXTRACT(EPOCH FROM
      (
        (SELECT (elem ->> 'updatedAt')::timestamp
         FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
         WHERE elem ->> 'title' = 'delivered'
         LIMIT 1)
        -
        (SELECT (elem ->> 'createdAt')::timestamp
         FROM jsonb_array_elements(o.orders_timeline::jsonb) AS elem
         WHERE elem ->> 'title' = 'accepted'
         LIMIT 1)
      )
    ) / 60
    ELSE NULL
  END AS fulfillment_time_minutes

FROM {{ ref('bv_orders') }} o
LEFT JOIN {{ ref('bv_customers') }} c ON o.orders_customer_id_fk = c.customer_id_pk
LEFT JOIN {{ ref('bv_branches') }} b ON o.orders_branch_id_fk = b.branches_id_pk
  
)

SELECT * FROM orders_timelines












