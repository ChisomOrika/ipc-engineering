{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  GoSource order frequency per customer.
  Tracks ordering cadence — widening gaps between orders
  is an early churn signal. One row per customer.
*/

WITH receipt_customer AS (
    SELECT
        COALESCE(r."customerId", r."business", b."businessId") AS customer_id,
        r."createdAt"::date AS order_date
    FROM raw_gosource.receipts r
    LEFT JOIN raw_gosource.branches b ON r."branch" = b."_id"
    WHERE LOWER(r."status") = 'delivered'
),

order_gaps AS (
    SELECT
        customer_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
        order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS days_between
    FROM receipt_customer
),

-- Recent vs earlier gap comparison
recent_gaps AS (
    SELECT
        customer_id,
        ROUND(AVG(days_between)) AS avg_gap_last_90d
    FROM order_gaps
    WHERE order_date >= CURRENT_DATE - 90
      AND days_between IS NOT NULL
    GROUP BY customer_id
),
earlier_gaps AS (
    SELECT
        customer_id,
        ROUND(AVG(days_between)) AS avg_gap_prior
    FROM order_gaps
    WHERE order_date < CURRENT_DATE - 90
      AND days_between IS NOT NULL
    GROUP BY customer_id
),

customer_summary AS (
    SELECT
        customer_id,
        COUNT(*)                          AS lifetime_orders,
        MIN(order_date)                   AS first_order_date,
        MAX(order_date)                   AS last_order_date,
        CURRENT_DATE - MAX(order_date)    AS days_since_last_order,
        ROUND(AVG(days_between))          AS avg_days_between_orders,
        MIN(days_between)                 AS min_gap_days,
        MAX(days_between)                 AS max_gap_days,

        -- Monthly order counts
        COUNT(*) FILTER (WHERE order_date >= CURRENT_DATE - 30)  AS orders_last_30d,
        COUNT(*) FILTER (WHERE order_date >= CURRENT_DATE - 90)  AS orders_last_90d
    FROM order_gaps
    GROUP BY customer_id
)

SELECT
    {{ clean_business_name('c."businessName"') }} AS business_name,
    cs.lifetime_orders,
    cs.first_order_date,
    cs.last_order_date,
    cs.days_since_last_order,
    cs.avg_days_between_orders,
    cs.min_gap_days,
    cs.max_gap_days,
    cs.orders_last_30d,
    cs.orders_last_90d,

    -- Frequency trend
    rg.avg_gap_last_90d,
    eg.avg_gap_prior,
    CASE
        WHEN eg.avg_gap_prior IS NOT NULL AND rg.avg_gap_last_90d IS NOT NULL
        THEN rg.avg_gap_last_90d - eg.avg_gap_prior
        ELSE NULL
    END AS gap_trend_days,

    -- Frequency status
    CASE
        WHEN cs.days_since_last_order > 60 THEN 'Dormant'
        WHEN rg.avg_gap_last_90d IS NOT NULL AND eg.avg_gap_prior IS NOT NULL
             AND rg.avg_gap_last_90d > eg.avg_gap_prior * 1.5 THEN 'Slowing Down'
        WHEN cs.orders_last_30d > 0 THEN 'Active'
        WHEN cs.orders_last_90d > 0 THEN 'Cooling Off'
        ELSE 'Dormant'
    END AS frequency_status,

    CURRENT_DATE AS snapshot_date

FROM customer_summary cs
JOIN raw_gosource.customers c ON c."_id" = cs.customer_id
LEFT JOIN recent_gaps rg  ON rg.customer_id = cs.customer_id
LEFT JOIN earlier_gaps eg ON eg.customer_id = cs.customer_id
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
ORDER BY cs.lifetime_orders DESC