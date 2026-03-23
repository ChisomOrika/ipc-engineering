{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH order quality per restaurant.
  Tracks rejection/void rates and reasons — helps customer service
  identify restaurants with operational issues.
  One row per customer per month.
*/

WITH orders AS (
    SELECT
        o."customer"       AS customer_id,
        DATE_TRUNC('month', o."createdAt"::date)::date AS order_month,
        o."status",
        o."rejectReason",
        o."rejectDescription"
    FROM raw_dash.orders o
    WHERE o."customer" IS NOT NULL
),

monthly AS (
    SELECT
        customer_id,
        order_month,
        COUNT(*)                                                      AS total_orders,
        COUNT(*) FILTER (WHERE LOWER(status) = 'delivered')           AS delivered,
        COUNT(*) FILTER (WHERE LOWER(status) = 'rejected')            AS rejected,
        COUNT(*) FILTER (WHERE LOWER(status) = 'voided')              AS voided,
        COUNT(*) FILTER (WHERE LOWER(status) NOT IN ('delivered', 'rejected', 'voided')) AS other_status,
        ROUND(COUNT(*) FILTER (WHERE LOWER(status) IN ('rejected', 'voided'))::numeric
              / NULLIF(COUNT(*), 0) * 100, 1)                         AS fail_rate_pct
    FROM orders
    GROUP BY customer_id, order_month
),

-- Top rejection reasons per customer (all-time)
rejection_reasons AS (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        "rejectReason" AS top_reject_reason,
        COUNT(*) AS reason_count
    FROM orders
    WHERE "rejectReason" IS NOT NULL
    GROUP BY customer_id, "rejectReason"
    ORDER BY customer_id, COUNT(*) DESC
)

SELECT
    m.*,
    {{ clean_business_name('c."businessName"') }} AS business_name,
    rr.top_reject_reason,
    rr.reason_count AS top_reason_count
FROM monthly m
JOIN raw_dash.customers c ON c."_id" = m.customer_id
LEFT JOIN rejection_reasons rr ON rr.customer_id = m.customer_id
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
ORDER BY m.customer_id, m.order_month