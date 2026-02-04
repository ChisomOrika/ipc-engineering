{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH products_details AS (

SELECT
  products_id_pk,
  products_name AS product_name,
  products_description,
  products_unit_price,
  products_customer_id_fk as customer_id_fk,
  products_branch_id_fk as branch_id_fk,
  products_active,
  products_created_at_date_time AS created_at,
  products_updated_at_date_time AS updated_at
FROM {{ ref('bv_products') }} c  
)

SELECT * FROM products_details
