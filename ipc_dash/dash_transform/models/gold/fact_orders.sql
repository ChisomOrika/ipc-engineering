{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH order_details AS (
  SELECT
    o.orders_id_pk,
    o.orders_customer_id_fk AS customer_id,
    c.customer_business_name AS customer_name,
    o.orders_branch_id_fk AS branch_id,
    b.branches_name AS branch_name,
    o.orders_order_type,
    o.orders_delivery_type,
    o.orders_status,
    o.orders_channel,
    (REPLACE(o.orders_address::text, 'NaN', 'null'))::jsonb ->> 'lga' AS lga,
    (REPLACE(o.orders_address::text, 'NaN', 'null'))::jsonb ->> 'state' AS state,
    o.orders_total_price_amount AS total_amount,
    o.orders_discount,
    o.orders_payment_method,
    o.orders_payment_status,
    o.orders_delivery_fee_amount,
    o.orders_service_charge_amount,
    o.orders_subtotal_amount,
    o.orders_created_at_date_time::date AS order_date,
    o.orders_created_at_date_time AS order_date_time
  FROM {{ ref('bv_orders') }} o
  LEFT JOIN {{ ref('bv_customers') }} c ON o.orders_customer_id_fk = c.customer_id_pk
  LEFT JOIN {{ ref('bv_branches') }} b ON o.orders_branch_id_fk = b.branches_id_pk
)
SELECT
  orders_id_pk,
  order_date,
  order_date_time,
  branch_id,
  branch_name,
  customer_id,
  customer_name,
  orders_order_type,
  orders_delivery_type,
  orders_status,
  orders_channel,
  lga,
  state,
  orders_payment_method,
  orders_payment_status,
  COUNT(orders_id_pk) AS total_orders,
  SUM(total_amount) AS total_sales,
  SUM(orders_discount) AS total_discount,
  SUM(orders_delivery_fee_amount) AS total_delivery_fee,
  SUM(orders_service_charge_amount) AS total_service_charge,
  SUM(orders_subtotal_amount) AS total_subtotal
FROM order_details
GROUP BY
  orders_id_pk,
  order_date,
  order_date_time,
  branch_id,
  branch_name,
  customer_id,
  customer_name,
  orders_order_type,
  orders_delivery_type,
  orders_status,
  orders_channel,
  lga,
  state,
  orders_payment_method,
  orders_payment_status  
ORDER BY order_date, branch_name

