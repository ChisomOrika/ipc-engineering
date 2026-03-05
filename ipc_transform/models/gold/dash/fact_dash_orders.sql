{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='order_id_pk'
) }}

SELECT
  o.order_id_pk,
  o.order_created_at_date_time::date AS order_date,
  o.order_created_at_date_time AS order_date_time,
  o.order_branch_id_fk AS branch_id,
  b.branch_name AS branch_name,
  o.order_customer_id_fk AS customer_id,
  c.customer_business_name AS customer_name,
  o.order_type,
  o.order_delivery_type,
  o.order_status,
  o.order_channel,
  (REPLACE(o.order_address::text, 'NaN', 'null'))::jsonb ->> 'lga' AS lga,
  (REPLACE(o.order_address::text, 'NaN', 'null'))::jsonb ->> 'state' AS state,
  o.order_payment_method,
  o.order_payment_status,
  COUNT(o.order_id_pk)               AS total_orders,
  SUM(o.order_total_price_amount)    AS total_sales,
  SUM(o.order_discount)              AS total_discount,
  SUM(o.order_delivery_fee_amount)   AS total_delivery_fee,
  SUM(o.order_service_charge_amount) AS total_service_charge,
  SUM(o.order_subtotal_amount)       AS total_subtotal
FROM {{ ref('bv_dash_orders') }} o 
LEFT JOIN {{ ref('bv_dash_customers') }} c ON o.order_customer_id_fk = c.customer_id_pk
LEFT JOIN {{ ref('bv_dash_branches') }} b ON o.order_branch_id_fk = b.branch_id_pk
{% if is_incremental() %}
  WHERE o.order_updated_at_date_time > (SELECT MAX(order_date_time) FROM {{ this }})
{% endif %}
GROUP BY
  o.order_id_pk,       
  order_date,
  order_date_time,
  branch_id,
  branch_name,
  customer_id,
  customer_name,
  o.order_type,
  o.order_delivery_type,
  o.order_status,
  o.order_channel,
  lga,
  state,
  o.order_payment_method,
  o.order_payment_status
ORDER BY order_date, branch_name