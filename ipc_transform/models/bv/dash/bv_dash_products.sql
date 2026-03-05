{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_products as (
  select
  "_id" as product_id_pk,
  "name" as product_name,
  "description" as product_description,
  "unitPrice" as product_unit_price,
  "totalPrice" as product_total_price,
  "inStock" as product_in_stock,
  "unit" as product_unit,
  "active" as product_active,
  "image" as product_image,
  "quantity" as product_quantity_count,
  "isLowStock" as product_is_low_stock,
  "lowStockLevel" as product_low_stock_level,
  "customer" as product_customer_id_fk,
  "branch" as product_branch_id_fk,
  "timeline" as product_timeline,
  "createdAt" as product_created_at_date_time,
  "updatedAt" as product_updated_at_date_time,
  "slug" as product_slug,
  "__v" as product___v,
  "newQuantity" as product_new_quantity_count,
  "record_load_date" as product_record_load_date
from {{ ref('raw_dash_products') }}

)

select *
from bv_products
