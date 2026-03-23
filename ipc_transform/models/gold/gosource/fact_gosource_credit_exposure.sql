{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  GoSource credit exposure (accounts receivable) per customer.
  Shows unpaid delivered orders with aging buckets.
  Critical for finance team — management-only dashboard.
  One row per customer.
*/

WITH receipt_customer AS (
    SELECT
        r."_id"            AS receipt_id,
        r."totalPrice"::numeric AS total_price,
        r."status",
        r."paymentMethod",
        r."paymentStatus",
        r."createdAt"::date AS order_date,
        COALESCE(r."customerId", r."business", b."businessId") AS customer_id
    FROM raw_gosource.receipts r
    LEFT JOIN raw_gosource.branches b ON r."branch" = b."_id"
    WHERE LOWER(r."status") = 'delivered'
),

unpaid AS (
    SELECT
        customer_id,
        receipt_id,
        total_price,
        order_date,
        CURRENT_DATE - order_date AS days_outstanding,
        CASE
            WHEN CURRENT_DATE - order_date <= 30  THEN '0-30d'
            WHEN CURRENT_DATE - order_date <= 60  THEN '31-60d'
            WHEN CURRENT_DATE - order_date <= 90  THEN '61-90d'
            ELSE '90d+'
        END AS aging_bucket
    FROM receipt_customer
    WHERE LOWER(COALESCE("paymentStatus", '')) != 'paid'
),

customer_summary AS (
    SELECT
        customer_id,

        -- Total exposure
        COUNT(*)                    AS unpaid_order_count,
        ROUND(SUM(total_price))     AS total_unpaid_amount,
        MIN(order_date)             AS oldest_unpaid_date,
        MAX(order_date)             AS newest_unpaid_date,
        ROUND(AVG(days_outstanding)) AS avg_days_outstanding,

        -- Aging buckets
        COUNT(*) FILTER (WHERE aging_bucket = '0-30d')   AS orders_0_30d,
        ROUND(SUM(total_price) FILTER (WHERE aging_bucket = '0-30d'))  AS amount_0_30d,
        COUNT(*) FILTER (WHERE aging_bucket = '31-60d')  AS orders_31_60d,
        ROUND(SUM(total_price) FILTER (WHERE aging_bucket = '31-60d')) AS amount_31_60d,
        COUNT(*) FILTER (WHERE aging_bucket = '61-90d')  AS orders_61_90d,
        ROUND(SUM(total_price) FILTER (WHERE aging_bucket = '61-90d')) AS amount_61_90d,
        COUNT(*) FILTER (WHERE aging_bucket = '90d+')    AS orders_90d_plus,
        ROUND(SUM(total_price) FILTER (WHERE aging_bucket = '90d+'))   AS amount_90d_plus
    FROM unpaid
    GROUP BY customer_id
),

-- Total delivered for payment rate context
total_delivered AS (
    SELECT
        customer_id,
        COUNT(*)                AS total_delivered_orders,
        ROUND(SUM(total_price)) AS total_delivered_amount,
        COUNT(*) FILTER (WHERE LOWER(COALESCE("paymentStatus", '')) = 'paid') AS paid_orders,
        ROUND(SUM(CASE WHEN LOWER(COALESCE("paymentStatus", '')) = 'paid' THEN total_price ELSE 0 END)) AS paid_amount
    FROM receipt_customer
    GROUP BY customer_id
)

SELECT
    {{ clean_business_name('c."businessName"') }} AS business_name,
    c."email"            AS email,
    c."canBuyOnCredit"   AS can_buy_on_credit,

    -- Lifetime totals
    COALESCE(td.total_delivered_orders, 0)  AS lifetime_delivered_orders,
    COALESCE(td.total_delivered_amount, 0)  AS lifetime_delivered_amount,
    COALESCE(td.paid_orders, 0)             AS lifetime_paid_orders,
    COALESCE(td.paid_amount, 0)             AS lifetime_paid_amount,

    -- Payment rate
    CASE WHEN COALESCE(td.total_delivered_amount, 0) > 0
         THEN ROUND(td.paid_amount::numeric / td.total_delivered_amount * 100, 1)
         ELSE NULL
    END AS lifetime_payment_rate_pct,

    -- Unpaid exposure
    COALESCE(cs.unpaid_order_count, 0)      AS unpaid_order_count,
    COALESCE(cs.total_unpaid_amount, 0)     AS total_unpaid_amount,
    cs.oldest_unpaid_date,
    cs.newest_unpaid_date,
    cs.avg_days_outstanding,

    -- Aging
    COALESCE(cs.orders_0_30d, 0)     AS orders_0_30d,
    COALESCE(cs.amount_0_30d, 0)     AS amount_0_30d,
    COALESCE(cs.orders_31_60d, 0)    AS orders_31_60d,
    COALESCE(cs.amount_31_60d, 0)    AS amount_31_60d,
    COALESCE(cs.orders_61_90d, 0)    AS orders_61_90d,
    COALESCE(cs.amount_61_90d, 0)    AS amount_61_90d,
    COALESCE(cs.orders_90d_plus, 0)  AS orders_90d_plus,
    COALESCE(cs.amount_90d_plus, 0)  AS amount_90d_plus,

    -- Risk level
    CASE
        WHEN COALESCE(cs.total_unpaid_amount, 0) = 0 THEN 'No Exposure'
        WHEN COALESCE(cs.amount_90d_plus, 0) > 0     THEN 'High Risk'
        WHEN COALESCE(cs.amount_61_90d, 0) > 0       THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS credit_risk_level,

    CURRENT_DATE AS snapshot_date

FROM raw_gosource.customers c
LEFT JOIN customer_summary cs    ON cs.customer_id = c."_id"
LEFT JOIN total_delivered td     ON td.customer_id = c."_id"
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
  AND COALESCE(td.total_delivered_orders, 0) > 0
ORDER BY cs.total_unpaid_amount DESC NULLS LAST