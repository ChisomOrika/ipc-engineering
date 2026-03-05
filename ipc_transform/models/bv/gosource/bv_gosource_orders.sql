{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='order_id_pk') }}

with raw_orders as (
    select *
    from {{ ref('raw_gosource_orders') }}
),

bv_orders as (
    select
        _id                                                     as order_id_pk,
        reference                                               as order_reference,
        "customerId"                                            as order_customer_id_fk,
        COALESCE("customerId", business)                        as order_unified_customer_id_fk,
        "businessName"                                          as order_business_name,
        "paymentMethod"                                         as order_payment_method,
        "paymentStatus"                                         as order_payment_status,
        paid                                                    as order_is_paid,
        "paymentCount"                                          as order_payment_count,
        status                                                  as order_status,
        ROUND(CAST(subtotal AS NUMERIC), 2)                     as order_subtotal_amount,
        "totalquantity"                                         as order_total_quantity,
        ROUND(CAST("deliveryFee" AS NUMERIC), 2)               as order_delivery_fee_amount,
        "serviceCharge"                                         as order_service_charge_amount,
        ROUND(CAST("totalPrice" AS NUMERIC), 2)                as order_total_price_amount,
        ROUND(CAST("additionalTotalPrice" AS NUMERIC), 2)      as order_additional_total_price_amount,
        discount                                                as order_discount_amount,
        coupon                                                  as order_coupon,
        "cancellationReason"                                    as order_cancellation_reason,
        "productId"                                             as order_product_id_fk,
        "product._id"                                           as order_product_id_fk_2,
        "product.name"                                          as order_product_name,
        "product.discountPrice"                                 as order_product_discount_price,
        "product.actualPrice"                                   as order_product_actual_price,
        "product.brand"                                         as order_product_brand,
        "product.category"                                      as order_product_category,
        unit                                                    as order_product_unit,
        quantity                                                as order_product_quantity,
        "createdAt"::date                                       as order_created_at_date,
        "updatedAt"::date                                       as order_updated_at_date,
        "deliveredAt"::date                                     as order_delivered_at_date,
        "shippedAt"::date                                       as order_shipped_at_date,
        "cancelledAt"::date                                     as order_cancelled_at_date,
        NULL::date                                              as order_record_load_date
    from raw_orders
    {% if is_incremental() %}
      WHERE "updatedAt"::date > (SELECT MAX(order_updated_at_date) FROM {{ this }})
    {% endif %}
)

select * from bv_orders
