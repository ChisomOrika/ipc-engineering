{{ config(materialized='table', schema='gold', tags=['Finance', 'Revenue']) }}

-- Unified revenue fact: DAASH (food delivery) + GoSource (B2B procurement)
-- service_line column enables cross-service filtering on a single dashboard
-- Surrogate key: md5(service_line || order_id) ensures global uniqueness

with daash_orders as (
    select
        o.order_id_pk,
        o.order_paystack_reference                                          as order_reference,
        o.order_customer_id_fk                                              as customer_id,
        coalesce(
            nullif(trim(c.customer_business_name), ''),
            nullif(trim(concat(c.customer_first_name, ' ', c.customer_last_name)), '')
        )                                                                   as customer_name,
        o.order_payment_method,
        o.order_payment_status,
        o.order_status,
        o.order_created_at_date_time::date                                  as order_date,
        null::date                                                          as delivered_date,
        o.order_subtotal_amount::numeric                                    as subtotal_amount,
        o.order_delivery_fee_amount::numeric                                as delivery_fee_amount,
        o.order_service_charge_amount::numeric                              as service_charge_amount,
        o.order_discount::numeric                                           as discount_amount,
        o.order_total_price_amount::numeric                                 as revenue_amount
    from {{ ref('bv_dash_orders') }} o
    left join {{ ref('bv_dash_customers') }} c
        on o.order_customer_id_fk = c.customer_id_pk
    where lower(o.order_status) = 'delivered'
),

gosource_orders as (
    select distinct on (order_id_pk)
        order_id_pk,
        order_reference,
        order_unified_customer_id_fk                                        as customer_id,
        order_business_name                                                 as customer_name,
        order_payment_method,
        order_payment_status,
        order_status,
        order_created_at_date                                               as order_date,
        order_delivered_at_date                                             as delivered_date,
        order_subtotal_amount                                               as subtotal_amount,
        order_delivery_fee_amount                                           as delivery_fee_amount,
        order_service_charge_amount                                         as service_charge_amount,
        order_discount_amount                                               as discount_amount,
        order_total_price_amount                                            as revenue_amount
    from {{ ref('bv_gosource_orders') }}
    where lower(order_status)         = 'delivered'
      and lower(order_payment_status) = 'paid'
    order by order_id_pk, order_delivered_at_date
)

select
    md5('DAASH' || order_id_pk)                                             as revenue_id_pk,
    'DAASH'                                                                 as service_line,
    order_id_pk                                                             as revenue_order_id,
    order_reference                                                         as revenue_order_reference,
    customer_id                                                             as revenue_customer_id_fk,
    customer_name                                                           as revenue_customer_name,
    order_payment_method                                                    as revenue_payment_method,
    order_payment_status                                                    as revenue_payment_status,
    order_status                                                            as revenue_order_status,
    order_date                                                              as revenue_order_date,
    delivered_date                                                          as revenue_delivered_date,
    date_trunc('month', order_date)::date                                   as revenue_month,
    date_trunc('year',  order_date)::date                                   as revenue_year,
    subtotal_amount                                                         as revenue_subtotal_amount,
    delivery_fee_amount                                                     as revenue_delivery_fee_amount,
    service_charge_amount                                                   as revenue_service_charge_amount,
    discount_amount                                                         as revenue_discount_amount,
    revenue_amount
from daash_orders

union all

select
    md5('GoSource' || order_id_pk)                                          as revenue_id_pk,
    'GoSource'                                                              as service_line,
    order_id_pk                                                             as revenue_order_id,
    order_reference                                                         as revenue_order_reference,
    customer_id                                                             as revenue_customer_id_fk,
    customer_name                                                           as revenue_customer_name,
    order_payment_method                                                    as revenue_payment_method,
    order_payment_status                                                    as revenue_payment_status,
    order_status                                                            as revenue_order_status,
    order_date                                                              as revenue_order_date,
    delivered_date                                                          as revenue_delivered_date,
    date_trunc('month', order_date)::date                                   as revenue_month,
    date_trunc('year',  order_date)::date                                   as revenue_year,
    subtotal_amount                                                         as revenue_subtotal_amount,
    delivery_fee_amount                                                     as revenue_delivery_fee_amount,
    service_charge_amount                                                   as revenue_service_charge_amount,
    discount_amount                                                         as revenue_discount_amount,
    revenue_amount
from gosource_orders
