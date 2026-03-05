{{ config(materialized='table', schema='gold', tags=['Finance', 'AR']) }}

-- Accounts Receivable aging for GoSource credit orders
-- Uses deliveredAt as the invoice date; aging calculated from current date
with credit_orders as (
    select
        order_id_pk,
        order_reference,
        order_unified_customer_id_fk,
        order_business_name,
        order_payment_method,
        order_payment_status,
        order_status,
        order_total_price_amount,
        order_delivered_at_date,
        order_created_at_date
    from {{ ref('bv_gosource_orders') }}
    where lower(order_payment_method) = 'credit'
      and lower(order_status)          = 'delivered'
      and lower(coalesce(order_payment_status, '')) != 'paid'
),

-- Deduplicate to order level
deduped as (
    select distinct on (order_id_pk)
        order_id_pk,
        order_reference,
        order_unified_customer_id_fk,
        order_business_name,
        order_payment_method,
        order_payment_status,
        order_total_price_amount,
        order_delivered_at_date,
        order_created_at_date
    from credit_orders
    order by order_id_pk, order_delivered_at_date
),

aged as (
    select
        *,
        current_date - order_delivered_at_date      as days_outstanding
    from deduped
    where order_delivered_at_date is not null
)

select
    order_id_pk                                                     as ar_order_id_pk,
    order_reference                                                 as ar_order_reference,
    order_unified_customer_id_fk                                    as ar_customer_id_fk,
    order_business_name                                             as ar_customer_name,
    order_payment_method                                            as ar_payment_method,
    order_payment_status                                            as ar_payment_status,
    order_created_at_date                                           as ar_order_date,
    order_delivered_at_date                                         as ar_invoice_date,
    days_outstanding                                                as ar_days_outstanding,

    -- Aging bucket
    case
        when days_outstanding between 0  and 30  then '0-30 days'
        when days_outstanding between 31 and 60  then '31-60 days'
        when days_outstanding between 61 and 90  then '61-90 days'
        when days_outstanding > 90               then '90+ days'
        else 'Unknown'
    end                                                             as ar_aging_bucket,

    -- Aging bucket sort order for dashboards
    case
        when days_outstanding between 0  and 30  then 1
        when days_outstanding between 31 and 60  then 2
        when days_outstanding between 61 and 90  then 3
        when days_outstanding > 90               then 4
        else 5
    end                                                             as ar_aging_bucket_sort,

    order_total_price_amount                                        as ar_outstanding_amount

from aged
