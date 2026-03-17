{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_members as (
  select
    "_id"            as member_id_pk,
    "branch"         as member_branch_id_fk,
    "customer"       as member_customer_id_fk,
    "email"          as member_email,
    "firstName"      as member_first_name,
    "lastName"       as member_last_name,
    "phoneNumber"    as member_phone_number,
    "position"       as member_position,
    "role"           as member_role,
    "verified"       as member_verified,
    "isDeactivated"  as member_is_deactivated,
    "createdAt"      as member_created_at,
    "updatedAt"      as member_updated_at,
    "__v"            as member___v,
    "record_load_date" as member_record_load_date
  from {{ ref('raw_dash_members') }}
)

select * from bv_members