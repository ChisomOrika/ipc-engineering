{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH order_product_details AS (

SELECT
  o.orders_id_pk,
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
  o.orders_customer_id_fk AS customer_id,
  o.orders_branch_id_fk AS branch_id,
  SUM((prod ->> 'quantity')::int) AS total_quantity,
  SUM((prod ->> 'price')::numeric) AS total_unit_price,
  SUM(((prod ->> 'price')::numeric * (prod ->> 'quantity')::int)) AS total_price
FROM {{ ref('bv_orders') }} o
CROSS JOIN LATERAL jsonb_array_elements(o.orders_products::jsonb) AS prod
LEFT JOIN {{ ref('bv_menucategories') }} mc 
  ON mc.menu_categories_id_pk = (prod -> 'productDetails' ->> 'category')
GROUP BY
  orders_id_pk,
  product_id,
  product_name,
  category_id,
  category_name,
  branch_id_1,
  customer_id_1,
  unit,
  customer_id,
  branch_id
ORDER BY total_quantity DESC
  
)

SELECT * FROM order_product_details



