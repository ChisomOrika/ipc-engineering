{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_menucategories as (
  select
  "_id" as menu_categories_id_pk,
  "name" as menu_categories_name,
  "description" as menu_categories_description,
  "slug" as menu_categories_slug,
  "customer" as menu_categories_customer_id_fk,
  "branch" as menu_categories_branch_id_fk,
  "createdAt" as menu_categories_created_at_date_time,
  "updatedAt" as menu_categories_updated_at_date_time,
  "__v" as menu_categories___v,
  "record_load_date" as menu_categories_record_load_date
from {{ ref('raw_menucategories') }}


)

select *
from bv_menucategories
