{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH average order value trend per restaurant per month.
  Tracks AOV direction — declining AOV can signal smaller baskets
  or loss of high-value menu items. Management-only metric.
  One row per customer per month.
*/

WITH delivered AS (
    SELECT
        o."customer"       AS customer_id,
        DATE_TRUNC('month', o."createdAt"::date)::date AS order_month,
        o."totalPrice"::numeric AS total_price
    FROM raw_dash.orders o
    WHERE LOWER(o."status") = 'delivered'
      AND o."customer" IS NOT NULL
      AND o."totalPrice" IS NOT NULL
),

monthly AS (
    SELECT
        customer_id,
        order_month,
        COUNT(*)                          AS order_count,
        ROUND(AVG(total_price))           AS avg_order_value,
        ROUND(MIN(total_price))           AS min_order_value,
        ROUND(MAX(total_price))           AS max_order_value,
        ROUND(SUM(total_price))           AS total_revenue
    FROM delivered
    GROUP BY customer_id, order_month
)

SELECT
    m.*,
    {{ clean_business_name('c."businessName"') }} AS business_name,

    -- Month-over-month AOV change
    LAG(m.avg_order_value) OVER (PARTITION BY m.customer_id ORDER BY m.order_month)
        AS prev_month_aov,
    CASE WHEN LAG(m.avg_order_value) OVER (PARTITION BY m.customer_id ORDER BY m.order_month) > 0
         THEN ROUND(
             (m.avg_order_value - LAG(m.avg_order_value) OVER (PARTITION BY m.customer_id ORDER BY m.order_month))::numeric
             / LAG(m.avg_order_value) OVER (PARTITION BY m.customer_id ORDER BY m.order_month) * 100, 1)
         ELSE NULL
    END AS aov_change_pct

FROM monthly m
JOIN raw_dash.customers c ON c."_id" = m.customer_id
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
ORDER BY m.customer_id, m.order_month