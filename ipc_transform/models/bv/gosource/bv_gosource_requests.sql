{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='request_id_pk') }}

with bv_requests as (
  select
    "_id"            as request_id_pk,
    "branch"         as request_branch_id_fk,
    "initiator"      as request_initiator_id,
    "status"         as request_status,
    "paymentMethod"  as request_payment_method,
    "deliveryFee"    as request_delivery_fee,
    "serviceCharge"  as request_service_charge,
    "reference"      as request_reference,
    "products"       as request_products,
    "createdAt"      as request_created_at,
    "updatedAt"      as request_updated_at
  from {{ ref('raw_gosource_requests') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(request_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_requests