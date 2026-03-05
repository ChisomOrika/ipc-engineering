{{ config(materialized='table', schema='gold', tags=['Finance', 'Profitability']) }}

-- IPC Group profitability by service line
--
-- DAASH (marketplace model):
--   No inventory owned — profit = service charge collected per order
--   COGS = 0 (restaurant/kitchen bears the food cost)
--
-- GoSource (procurement model):
--   IPC owns inventory — profit = revenue minus product cost
--   COGS = sum(actual_price × quantity) across all product lines per order
--   Service charge = IPC's explicit margin on top of product cost

-- ── DAASH ─────────────────────────────────────────────────────────────────
with daash_orders as (
    select
        o.order_id_pk,
        o.order_paystack_reference                                          as order_reference,
        o.order_customer_id_fk                                             as customer_id,
        coalesce(
            nullif(trim(c.customer_business_name), ''),
            nullif(trim(concat(c.customer_first_name, ' ', c.customer_last_name)), '')
        )                                                                   as customer_name,
        o.order_created_at_date_time::date                                  as profit_date,
        coalesce(o.order_total_price_amount::numeric,    0)                 as revenue_amount,
        coalesce(o.order_subtotal_amount::numeric,       0)                 as subtotal_amount,
        coalesce(o.order_delivery_fee_amount::numeric,   0)                 as delivery_fee_amount,
        coalesce(o.order_service_charge_amount::numeric, 0)                 as service_charge_amount,
        coalesce(o.order_discount::numeric,              0)                 as discount_amount,
        0::numeric                                                          as cogs_amount,
        -- Marketplace: profit = service charge (restaurant owns food cost)
        coalesce(o.order_service_charge_amount::numeric, 0)                 as gross_profit_amount
    from {{ ref('bv_dash_orders') }} o
    left join {{ ref('bv_dash_customers') }} c
        on o.order_customer_id_fk = c.customer_id_pk
    where lower(o.order_status) = 'delivered'
),

-- ── GoSource ──────────────────────────────────────────────────────────────
-- Step 1: order-level revenue fields (deduplicated — source is product-level)
gosource_revenue as (
    select distinct on (order_id_pk)
        order_id_pk,
        order_reference,
        order_unified_customer_id_fk                                        as customer_id,
        order_business_name                                                 as customer_name,
        order_created_at_date                                               as profit_date,
        coalesce(order_total_price_amount,    0)                            as revenue_amount,
        coalesce(order_subtotal_amount,       0)                            as subtotal_amount,
        coalesce(order_delivery_fee_amount,   0)                            as delivery_fee_amount,
        coalesce(order_service_charge_amount, 0)                            as service_charge_amount,
        coalesce(order_discount_amount,       0)                            as discount_amount
    from {{ ref('bv_gosource_orders') }}
    where lower(order_status)         = 'delivered'
      and lower(order_payment_status) = 'paid'
    order by order_id_pk, order_delivered_at_date
),

-- Step 2: COGS per order — sum actual cost across all product lines
gosource_cogs as (
    select
        order_id_pk,
        sum(
            coalesce(order_product_actual_price::numeric, 0) *
            coalesce(order_product_quantity::numeric,     1)
        )                                                                   as cogs_amount
    from {{ ref('bv_gosource_orders') }}
    where lower(order_status)         = 'delivered'
      and lower(order_payment_status) = 'paid'
    group by order_id_pk
),

gosource_orders as (
    select
        r.order_id_pk,
        r.order_reference,
        r.customer_id,
        r.customer_name,
        r.profit_date,
        r.revenue_amount,
        r.subtotal_amount,
        r.delivery_fee_amount,
        r.service_charge_amount,
        r.discount_amount,
        coalesce(c.cogs_amount, 0)                                          as cogs_amount,
        -- Procurement: gross profit = what customer paid minus what IPC paid suppliers
        r.revenue_amount - coalesce(c.cogs_amount, 0)                       as gross_profit_amount
    from gosource_revenue r
    left join gosource_cogs c on r.order_id_pk = c.order_id_pk
)

-- ── Final union ───────────────────────────────────────────────────────────
select
    md5('DAASH' || order_id_pk)                                             as profitability_id_pk,
    'DAASH'                                                                 as service_line,
    order_id_pk                                                             as profit_order_id,
    order_reference                                                         as profit_order_reference,
    customer_id                                                             as profit_customer_id_fk,
    customer_name                                                           as profit_customer_name,
    profit_date,
    date_trunc('month', profit_date)::date                                  as profit_month,
    date_trunc('year',  profit_date)::date                                  as profit_year,
    revenue_amount                                                          as profit_revenue_amount,
    subtotal_amount                                                         as profit_subtotal_amount,
    delivery_fee_amount                                                     as profit_delivery_fee_amount,
    service_charge_amount                                                   as profit_service_charge_amount,
    discount_amount                                                         as profit_discount_amount,
    cogs_amount                                                             as profit_cogs_amount,
    gross_profit_amount                                                     as profit_gross_profit_amount,
    -- Gross margin % (how much of each ₦ of revenue is profit)
    case
        when revenue_amount > 0
        then round(gross_profit_amount / revenue_amount * 100, 2)
        else 0
    end                                                                     as profit_gross_margin_pct
from daash_orders

union all

select
    md5('GoSource' || order_id_pk)                                          as profitability_id_pk,
    'GoSource'                                                              as service_line,
    order_id_pk                                                             as profit_order_id,
    order_reference                                                         as profit_order_reference,
    customer_id                                                             as profit_customer_id_fk,
    customer_name                                                           as profit_customer_name,
    profit_date,
    date_trunc('month', profit_date)::date                                  as profit_month,
    date_trunc('year',  profit_date)::date                                  as profit_year,
    revenue_amount                                                          as profit_revenue_amount,
    subtotal_amount                                                         as profit_subtotal_amount,
    delivery_fee_amount                                                     as profit_delivery_fee_amount,
    service_charge_amount                                                   as profit_service_charge_amount,
    discount_amount                                                         as profit_discount_amount,
    cogs_amount                                                             as profit_cogs_amount,
    gross_profit_amount                                                     as profit_gross_profit_amount,
    case
        when revenue_amount > 0
        then round(gross_profit_amount / revenue_amount * 100, 2)
        else 0
    end                                                                     as profit_gross_margin_pct
from gosource_orders
