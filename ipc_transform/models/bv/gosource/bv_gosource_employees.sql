{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='employee_id_pk') }}

with bv_employees as (
  select
    "_id"            as employee_id_pk,
    "businessId"     as employee_business_id_fk,
    "branchId"       as employee_branch_id_fk,
    "firstName"      as employee_first_name,
    "lastName"       as employee_last_name,
    "email"          as employee_email,
    "role"           as employee_role,
    "position"       as employee_position,
    "isDeactivated"  as employee_is_deactivated,
    "verified"       as employee_verified,
    "createdAt"      as employee_created_at,
    "updatedAt"      as employee_updated_at
  from {{ ref('raw_gosource_employees') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(employee_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_employees