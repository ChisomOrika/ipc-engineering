{{ config(materialized='incremental', schema='bv', tags=['DAASH'], unique_key='activity_id_pk') }}

with bv_activitylogs as (
  select
    "_id"            as activity_id_pk,
    "branch"         as activity_branch_id_fk,
    "customer"       as activity_customer_id_fk,
    "description"    as activity_description,
    "initiator"      as activity_initiator_id,
    "initiatorType"  as activity_initiator_type,
    "createdAt"      as activity_created_at,
    "updatedAt"      as activity_updated_at,
    "__v"            as activity___v,
    "record_load_date" as activity_record_load_date
  from {{ ref('raw_dash_activitylogs') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(activity_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_activitylogs