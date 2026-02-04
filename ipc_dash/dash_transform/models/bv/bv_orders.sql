{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_orders as (
  select
  "_id" as orders_id_pk,
  "products" as orders_products,
  "paymentMethod" as orders_payment_method,
  "paymentStatus" as orders_payment_status,
  "deliveryFee" as orders_delivery_fee_amount,
  "serviceCharge" as orders_service_charge_amount,
  "subtotal" as orders_subtotal_amount,
  "reference" as orders_reference,
  "phoneNumber" as orders_phone_number,
  "paymentReference" as orders_payment_reference,
  "status" as orders_status,
  "channel" as orders_channel,
  "user" as orders_user,
  "customer" as orders_customer_id_fk,
  "branch" as orders_branch_id_fk,
  "totalPrice" as orders_total_price_amount,
  "orderType" as orders_order_type,
  "deliveryType" as orders_delivery_type,
  "address" as orders_address,
  "timeline" as orders_timeline,
  "customerNote" as orders_customer_note,
  "createdAt" as orders_created_at_date_time,
  "updatedAt" as orders_updated_at_date_time,
  "__v" as orders___v,
  "paystackReference" as orders_paystack_reference,
  "duration" as orders_duration,
  "discount" as orders_discount,
  "scheduleDetails" as orders_schedule_details,
  "awaitingRefund" as orders_awaiting_refund,
  "tax" as orders_tax,
  "member" as orders_member_id_fk,
  "statusCheckCount" as orders_status_check_count,
  "ratings" as orders_ratings,
  "rejectDescription" as orders_reject_description,
  "rejectReason" as orders_reject_reason,
  "gift" as orders_gift,
  "giftDetails" as orders_gift_details,
  "deliveryArea" as orders_delivery_area,
  "package" as orders_package,
  "record_load_date" as orders_record_load_date
from {{ ref('raw_orders') }}



)

select *
from bv_orders
