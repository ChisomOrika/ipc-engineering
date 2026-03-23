{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='po_id_pk') }}

with bv_purchaseorders as (
  select
    "_id"            as po_id_pk,
    "creator"        as po_creator_id,
    "status"         as po_status,
    "productType"    as po_product_type,
    "expectedDate"   as po_expected_date,
    "note"           as po_note,
    "products"       as po_products,
    "suppliers"      as po_suppliers,
    "createdAt"      as po_created_at,
    "updatedAt"      as po_updated_at
  from {{ ref('raw_gosource_purchaseorders') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(po_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_purchaseorders