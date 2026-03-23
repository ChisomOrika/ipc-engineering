{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='branch_id_pk') }}

with bv_branches as (
  select
    "_id"            as branch_id_pk,
    "businessId"     as branch_business_id_fk,
    "branchName"     as branch_name,
    "branchCode"     as branch_code,
    "isHeadquarter"  as branch_is_headquarter,
    "isDeactivated"  as branch_is_deactivated,
    "streetName"     as branch_street,
    "lga"            as branch_lga,
    "createdAt"      as branch_created_at,
    "updatedAt"      as branch_updated_at
  from {{ ref('raw_gosource_branches') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(branch_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_branches