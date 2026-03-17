{{ config(materialized='incremental', schema='bv', tags=['DAASH'], unique_key='menuitem_id_pk') }}

with bv_menuitems as (
  select
    "_id"            as menuitem_id_pk,
    "branch"         as menuitem_branch_id_fk,
    "customer"       as menuitem_customer_id_fk,
    "category"       as menuitem_category_id_fk,
    "name"           as menuitem_name,
    "description"    as menuitem_description,
    "price"          as menuitem_price,
    "image"          as menuitem_image,
    "slug"           as menuitem_slug,
    "active"         as menuitem_active,
    "outOfStock"     as menuitem_out_of_stock,
    "ingredients"    as menuitem_ingredients,
    "modifiers"      as menuitem_modifiers,
    "createdAt"      as menuitem_created_at,
    "updatedAt"      as menuitem_updated_at,
    "__v"            as menuitem___v,
    "record_load_date" as menuitem_record_load_date
  from {{ ref('raw_dash_menuitems') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(menuitem_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_menuitems