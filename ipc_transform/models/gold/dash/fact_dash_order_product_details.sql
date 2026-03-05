
{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='order_id_pk'
) }}

SELECT
  o.order_id_pk,
  (prod ->> 'product') AS product_id,
  (prod -> 'productDetails' ->> 'name') AS product_name,
  (prod -> 'productDetails' ->> 'category') AS category_id,
  mc.menu_categories_name AS category_name,
  (prod -> 'productDetails' ->> 'branch') AS branch_id_1,
  (prod -> 'productDetails' ->> 'customer') AS customer_id_1,
  COALESCE(
    (prod ->> 'unit'),
    (prod -> 'productDetails' ->> 'unit')
  ) AS unit,
  o.order_customer_id_fk AS customer_id,
  o.order_branch_id_fk AS branch_id,
  o.order_updated_at_date_time,
  SUM((prod ->> 'quantity')::int) AS total_quantity,
  SUM((prod ->> 'price')::numeric) AS total_unit_price,
  SUM(((prod ->> 'price')::numeric * (prod ->> 'quantity')::int)) AS total_price
FROM {{ ref('bv_dash_orders') }} o
CROSS JOIN LATERAL jsonb_array_elements(o.order_products::jsonb) AS prod
LEFT JOIN {{ ref('bv_dash_menucategories') }} mc 
  ON mc.menu_categories_id_pk = (prod -> 'productDetails' ->> 'category')
{% if is_incremental() %}
  WHERE o.order_updated_at_date_time > (SELECT MAX(t.order_updated_at_date_time)  FROM {{ this }} t)
{% endif %}
GROUP BY
  o.order_id_pk,
  product_id,
  product_name,
  category_id,
  category_name,
  branch_id_1,
  customer_id_1,
  unit,
  customer_id,
  branch_id,
  o.order_updated_at_date_time
ORDER BY total_quantity DESC
