{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='movement_id_pk') }}

with bv_inventorymovements as (
  select
    "_id"            as movement_id_pk,
    "branch"         as movement_branch_id_fk,
    "product"        as movement_product_id_fk,
    "movementType"   as movement_type,
    "quantity"        as movement_quantity,
    "runningBalance"  as movement_running_balance,
    "unit"           as movement_unit,
    "description"    as movement_description,
    "reference"      as movement_reference,
    "movementDate"   as movement_date,
    "createdAt"      as movement_created_at,
    "updatedAt"      as movement_updated_at
  from {{ ref('raw_gosource_inventorymovements') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(movement_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_inventorymovements