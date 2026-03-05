{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='product_id_pk'
) }}

WITH products_details AS (
  SELECT
    product_id_pk,
    product_name AS product_name,
    product_description,
    product_unit_price,
    product_customer_id_fk AS customer_id_fk,
    product_branch_id_fk AS branch_id_fk,
    product_active,
    product_created_at_date_time AS product_created_at,
    product_updated_at_date_time AS product_updated_at
  FROM {{ ref('bv_dash_products') }}
  {% if is_incremental() %}
    WHERE product_updated_at_date_time > (SELECT MAX(product_updated_at) FROM {{ this }})
  {% endif %}
)

SELECT * FROM products_details