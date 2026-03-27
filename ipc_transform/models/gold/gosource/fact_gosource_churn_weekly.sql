{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  Weekly churn detection (Sunday-based weeks: Sun → Sat).
  GoSource version — uses receipts-based customer resolution.

  "Churned this week" = had delivered orders LAST week but ZERO this week.
  "Reactivated" = had zero orders last week but placed orders this week,
                  AND was active in some earlier week.
  "Active" = had orders both last week and this week.

  Only includes brands relevant to the current week boundary.
*/

WITH current_week AS (
    SELECT
        (DATE_TRUNC('week', CURRENT_DATE + 1)::date - 1) AS this_week_start,
        (DATE_TRUNC('week', CURRENT_DATE + 1)::date - 1) + 6 AS this_week_end,
        (DATE_TRUNC('week', CURRENT_DATE + 1)::date - 8) AS last_week_start,
        (DATE_TRUNC('week', CURRENT_DATE + 1)::date - 2) AS last_week_end
),

brand_ids AS (
    SELECT
        c."_id" AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name
    FROM raw_gosource.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
),

-- Deduplicate orders to order-level
deduped_orders AS (
    SELECT DISTINCT ON (_id) *
    FROM raw_gosource.orders
    ORDER BY _id, "updatedAt" DESC
),

brand_orders AS (
    SELECT
        bi.business_name,
        COUNT(*) FILTER (
            WHERE o."createdAt"::date BETWEEN (SELECT this_week_start FROM current_week)
                                          AND (SELECT this_week_end FROM current_week)
        ) AS orders_this_week,
        COUNT(*) FILTER (
            WHERE o."createdAt"::date BETWEEN (SELECT last_week_start FROM current_week)
                                          AND (SELECT last_week_end FROM current_week)
        ) AS orders_last_week,
        SUM(o."totalPrice"::numeric) FILTER (
            WHERE o."createdAt"::date BETWEEN (SELECT last_week_start FROM current_week)
                                          AND (SELECT last_week_end FROM current_week)
        ) AS revenue_last_week,
        MAX(o."createdAt"::date) AS last_order_date,
        -- Check if brand was active in any week before last week
        COUNT(*) FILTER (
            WHERE o."createdAt"::date < (SELECT last_week_start FROM current_week)
        ) AS orders_before_last_week
    FROM deduped_orders o
    INNER JOIN brand_ids bi ON bi.customer_id = COALESCE(o."business._id", o.business)
    WHERE LOWER(o.status) = 'delivered'
    GROUP BY bi.business_name
)

SELECT
    business_name,
    orders_this_week,
    orders_last_week,
    revenue_last_week,
    last_order_date,
    CURRENT_DATE - last_order_date AS days_since_last_order,

    CASE
        WHEN orders_last_week > 0 AND orders_this_week = 0
        THEN 'Churned'
        WHEN orders_last_week = 0 AND orders_this_week > 0 AND orders_before_last_week > 0
        THEN 'Reactivated'
        WHEN orders_this_week > 0
        THEN 'Active'
        ELSE 'Dormant'
    END AS weekly_status,

    (SELECT this_week_start FROM current_week) AS week_start,
    (SELECT this_week_end FROM current_week) AS week_end,
    CURRENT_DATE AS snapshot_date

FROM brand_orders
WHERE orders_last_week > 0 OR orders_this_week > 0
