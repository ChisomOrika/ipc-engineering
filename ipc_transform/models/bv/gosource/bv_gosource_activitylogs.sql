{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='activity_id_pk') }}

with bv_activitylogs as (
  select
    "_id"            as activity_id_pk,
    "action"         as activity_action,
    "module"         as activity_module,
    "description"    as activity_description,
    "initiator"      as activity_initiator_id,
    "initiatorType"  as activity_initiator_type,
    "objectId"       as activity_object_id,
    "createdAt"      as activity_created_at,
    "updatedAt"      as activity_updated_at
  from {{ ref('raw_gosource_activitylogs') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(activity_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_activitylogs