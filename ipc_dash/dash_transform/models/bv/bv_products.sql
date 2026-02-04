{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_products as (
  select
  "_id" as products_id_pk,
  "name" as products_name,
  "description" as products_description,
  "unitPrice" as products_unit_price,
  "totalPrice" as products_total_price,
  "inStock" as products_in_stock,
  "unit" as products_unit,
  "active" as products_active,
  "image" as products_image,
  "quantity" as products_quantity_count,
  "isLowStock" as products_is_low_stock,
  "lowStockLevel" as products_low_stock_level,
  "customer" as products_customer_id_fk,
  "branch" as products_branch_id_fk,
  "timeline" as products_timeline,
  "createdAt" as products_created_at_date_time,
  "updatedAt" as products_updated_at_date_time,
  "slug" as products_slug,
  "__v" as products___v,
  "newQuantity" as products_new_quantity_count,
  "record_load_date" as products_record_load_date
from {{ ref('raw_products') }}

)

select *
from bv_products
