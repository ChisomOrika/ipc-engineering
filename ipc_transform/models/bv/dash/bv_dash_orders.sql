{{ config(materialized='incremental', schema='bv', tags=['DAASH'], unique_key='order_id_pk') }}

with bv_orders as (
  select
  "_id" as order_id_pk,
  "products" as order_products,
  "paymentMethod" as order_payment_method,
  "paymentStatus" as order_payment_status,
  "deliveryFee" as order_delivery_fee_amount,
  "serviceCharge" as order_service_charge_amount,
  "subtotal" as order_subtotal_amount,
  "reference" as order_reference,
  "phoneNumber" as order_phone_number,
  "paymentReference" as order_payment_reference,
  "status" as order_status,
  "channel" as order_channel,
  "user" as order_user_id_fk,
  "customer" as order_customer_id_fk,
  "branch" as order_branch_id_fk,
  "totalPrice" as order_total_price_amount,
  "orderType" as order_type,
  "deliveryType" as order_delivery_type,
  "address" as order_address,
  "timeline" as order_timeline,
  "customerNote" as order_customer_note,
  "createdAt" as order_created_at_date_time,
  "updatedAt" as order_updated_at_date_time,
  "__v" as order___v,
  "paystackReference" as order_paystack_reference,
  "duration" as order_duration,
  "discount" as order_discount,
  "scheduleDetails" as order_schedule_details,
  "awaitingRefund" as order_awaiting_refund,
  "tax" as order_tax,
  "member" as order_member_id_fk,
  "statusCheckCount" as order_status_check_count,
  "ratings" as order_ratings,
  "rejectDescription" as order_reject_description,
  "rejectReason" as order_reject_reason,
  "gift" as order_gift,
  "giftDetails" as order_gift_details,
  "deliveryArea" as order_delivery_area,
  "package" as order_package,
  "record_load_date" as order_record_load_date
from {{ ref('raw_dash_orders') }}
{% if is_incremental() %}
  WHERE "updatedAt" > (SELECT MAX(order_updated_at_date_time) FROM {{ this }})
{% endif %}



)

select *
from bv_orders
