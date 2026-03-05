{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='receipts_id_pk') }}

with raw_receipts as (
    select * 
    from {{ ref('raw_gosource_receipts') }}  -- Reference the raw data in staging
),

bv_receipts as (SELECT
       _id                                          as receipts_id_pk,
       "customerId"                                 as receipts_customer_id_fk,
       COALESCE("customerId", business)             as unified_customer_id_fk,
       "paymentMethod"                              as receipts_payment_method,
       subtotal                                     as receipts_subtotal_amount,
       quantity                                     as receipts_total_quantity,
       ROUND(CAST("deliveryFee" AS NUMERIC), 2)     as receipts_delivery_fee_amount,
       "serviceCharge"                              as receipts_service_charge_amount,
       ROUND(CAST("totalPrice" AS NUMERIC), 2)      as receipts_total_price_amount,
       status                                       as receipts_status,
       coupon                                       as receipts_coupon,
       "createdAt"                                  as receipts_created_at_date,
       "updatedAt"::date                            as receipts_updated_at_date,
       "deliveredAt"::date                          as receipts_delivered_at_date,
       "shippedAt"::date                            as receipts_shipped_at_date,
       "cancelledAt"::date                          as receipts_cancelled_at_date from raw_receipts r
    {% if is_incremental() %}
      WHERE "updatedAt"::date > (SELECT MAX(receipts_updated_at_date) FROM {{ this }})
    {% endif %}
)

SELECT *
from bv_receipts













