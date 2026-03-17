{{ config(materialized='incremental', schema='bv', tags=['DAASH'], unique_key='visit_id_pk') }}

with bv_websitevisits as (
  select
    "_id"            as visit_id_pk,
    "branch"         as visit_branch_id_fk,
    "customer"       as visit_customer_id_fk,
    "subwebsite"     as visit_subwebsite_id_fk,
    "visits"         as visit_count,
    "createdAt"      as visit_created_at,
    "updatedAt"      as visit_updated_at,
    "__v"            as visit___v,
    "record_load_date" as visit_record_load_date
  from {{ ref('raw_dash_websitevisits') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(visit_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_websitevisits