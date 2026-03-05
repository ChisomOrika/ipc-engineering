{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='order_id'
) }}

SELECT
  o.order_id_pk AS order_id,
  o.order_customer_id_fk AS customer_id,
  c.customer_business_name AS customer_name,
  o.order_branch_id_fk AS branch_id,
  b.branch_name,
  o.order_user_id_fk AS user_id,
  o.order_type AS order_type,
  o.order_delivery_type AS delivery_type,
  o.order_status AS status,
  o.order_channel AS channel,
  o.order_duration AS duration,
  o.order_created_at_date_time AS order_created_at,
  o.order_updated_at_date_time AS order_updated_at,

  (
    SELECT (elem ->> 'createdAt')::timestamp
    FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'accepted'
    LIMIT 1
  ) AS accepted_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'preparing'
    LIMIT 1
  ) AS preparing_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'ready'
    LIMIT 1
  ) AS ready_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'shipped'
    LIMIT 1
  ) AS shipped_at,

  (
    SELECT (elem ->> 'updatedAt')::timestamp
    FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
    WHERE elem ->> 'title' = 'delivered'
    LIMIT 1
  ) AS delivered_at,

  CASE 
    WHEN (
      SELECT (elem ->> 'createdAt')::timestamp
      FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
      WHERE elem ->> 'title' = 'accepted'
      LIMIT 1
    ) IS NOT NULL
    AND (
      SELECT (elem ->> 'updatedAt')::timestamp
      FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
      WHERE elem ->> 'title' = 'delivered'
      LIMIT 1
    ) IS NOT NULL
    THEN EXTRACT(EPOCH FROM
      (
        (SELECT (elem ->> 'updatedAt')::timestamp
         FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
         WHERE elem ->> 'title' = 'delivered'
         LIMIT 1)
        -
        (SELECT (elem ->> 'createdAt')::timestamp
         FROM jsonb_array_elements(o.order_timeline::jsonb) AS elem
         WHERE elem ->> 'title' = 'accepted'
         LIMIT 1)
      )
    ) / 60
    ELSE NULL
  END AS fulfillment_time_minutes

FROM {{ ref('bv_dash_orders') }} o
LEFT JOIN {{ ref('bv_dash_customers') }} c ON o.order_customer_id_fk = c.customer_id_pk
LEFT JOIN {{ ref('bv_dash_branches') }} b ON o.order_branch_id_fk = b.branch_id_pk
{% if is_incremental() %}
  WHERE o.order_updated_at_date_time > (SELECT MAX(order_updated_at) FROM {{ this }})
{% endif %}
