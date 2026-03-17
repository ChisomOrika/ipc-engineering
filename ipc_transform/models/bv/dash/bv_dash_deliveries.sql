{{ config(materialized='incremental', schema='bv', tags=['DAASH'], unique_key='delivery_id_pk') }}

with bv_deliveries as (
  select
    "_id"                      as delivery_id_pk,
    "branch"                   as delivery_branch_id_fk,
    "customer"                 as delivery_customer_id_fk,
    "reference"                as delivery_reference,
    "name"                     as delivery_recipient_name,
    "email"                    as delivery_recipient_email,
    "address"                  as delivery_address,
    "distance"                 as delivery_distance,
    "deliveryFee"              as delivery_fee_amount,
    "feePerKm"                 as delivery_fee_per_km,
    "status"                   as delivery_status,
    "paymentStatus"            as delivery_payment_status,
    "logisticsId"              as delivery_logistics_id,
    "logisticsBusinessDetails" as delivery_logistics_details,
    "riderDetails"             as delivery_rider_details,
    "timeline"                 as delivery_timeline,
    "createdAt"                as delivery_created_at,
    "updatedAt"                as delivery_updated_at,
    "__v"                      as delivery___v,
    "record_load_date"         as delivery_record_load_date
  from {{ ref('raw_dash_deliveries') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(delivery_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_deliveries