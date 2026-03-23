{{ config(materialized='table', schema='gold', tags=['Finance', 'Profitability']) }}

-- IPC Group profitability by service line
--
-- DAASH (marketplace model):
--   No inventory owned — profit = service charge / platform fee per order
--   COGS = 0 (restaurant/kitchen bears the food cost)
--
-- GoSource (procurement model):
--   Revenue  = subtotal (discountPrice × qty) + service charge
--   COGS     = subtotal (product cost passed through — supplier cost not tracked per order)
--   GP       = service charge collected (IPC's explicit margin, applied to credit orders)
--   Note: supplier cost per unit is not recorded in the system, so GP = service charge only

-- ── DAASH ─────────────────────────────────────────────────────────────────
-- Platform fee per order comes from bv_dash_revenueledgers (revenue_ledger credits),
-- joined to orders by paystack reference. order_service_charge_amount in the orders
-- table is largely unpopulated — the ledger is the authoritative source.
with daash_platform_fee as (
    select
        revenue_ledgers_reference                                           as paystack_ref,
        sum(revenue_ledgers_amount)                                         as platform_fee
    from {{ ref('bv_dash_revenueledgers') }}
    where revenue_ledgers_type = 'credit'
      and revenue_ledgers_reference is not null
    group by revenue_ledgers_reference
),

daash_orders as (
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
        -- Service charge recorded in ledger (authoritative) or fallback to order field
        coalesce(f.platform_fee,
                 o.order_service_charge_amount::numeric, 0)                 as service_charge_amount,
        coalesce(o.order_discount::numeric,              0)                 as discount_amount,
        0::numeric                                                          as cogs_amount,
        -- GP = platform fee from revenue ledger (IPC's cut per delivered order)
        coalesce(f.platform_fee,
                 o.order_service_charge_amount::numeric, 0)                 as gross_profit_amount
    from {{ ref('bv_dash_orders') }} o
    left join {{ ref('bv_dash_customers') }} c
        on o.order_customer_id_fk = c.customer_id_pk
    left join daash_platform_fee f
        on o.order_paystack_reference = f.paystack_ref
    where lower(o.order_status) = 'delivered'
),

-- ── GoSource ──────────────────────────────────────────────────────────────
-- Deduplicated to order level (source table is product-level, one row per line item)
gosource_orders as (
    select distinct on (order_id_pk)
        order_id_pk,
        order_reference,
        order_unified_customer_id_fk                                        as customer_id,
        order_business_name                                                 as customer_name,
        order_created_at_date                                               as profit_date,
        coalesce(nullif(order_total_price_amount,    'NaN'::numeric), 0)      as revenue_amount,
        coalesce(nullif(order_subtotal_amount,       'NaN'::numeric), 0)    as subtotal_amount,
        coalesce(nullif(order_delivery_fee_amount,   'NaN'::numeric), 0)    as delivery_fee_amount,
        coalesce(nullif(order_service_charge_amount, 'NaN'::numeric), 0)    as service_charge_amount,
        coalesce(nullif(order_discount_amount,       'NaN'::numeric), 0)    as discount_amount,
        -- COGS = 0; supplier cost per unit not recorded in the system
        0::numeric                                                          as cogs_amount,
        -- GP = service charge only (IPC's explicit margin, applied on credit orders)
        coalesce(nullif(order_service_charge_amount, 'NaN'::numeric), 0)    as gross_profit_amount
    from {{ ref('bv_gosource_orders') }}
    where lower(order_status)         = 'delivered'
      and lower(order_payment_status) = 'paid'
    order by order_id_pk, order_delivered_at_date
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
        then round((gross_profit_amount / revenue_amount * 100)::numeric, 2)
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
        then round((gross_profit_amount / revenue_amount * 100)::numeric, 2)
        else 0
    end                                                                     as profit_gross_margin_pct
from gosource_orders
