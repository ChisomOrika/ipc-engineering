{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='invite_id_pk') }}

with bv_employeeinvites as (
  select
    "_id"            as invite_id_pk,
    "businessId"     as invite_business_id_fk,
    "branchId"       as invite_branch_id_fk,
    "email"          as invite_email,
    "role"           as invite_role,
    "status"         as invite_status,
    "createdAt"      as invite_created_at,
    "updatedAt"      as invite_updated_at
  from {{ ref('raw_gosource_employeeinvites') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(invite_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_employeeinvites