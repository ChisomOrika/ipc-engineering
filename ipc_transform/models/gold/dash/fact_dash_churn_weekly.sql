{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  Weekly churn detection.
  A brand is "churned this week" if:
    - They had delivered orders in the 30 days BEFORE this week
    - They had ZERO delivered orders in the last 7 days

  Also flags reactivations: brands that had no orders in prior 30d
  but placed orders this week.
*/

WITH brand_orders AS (
    SELECT
        o."customer"  AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name,
        COUNT(*) FILTER (WHERE o."createdAt"::date >= CURRENT_DATE - 7)   AS orders_this_week,
        COUNT(*) FILTER (WHERE o."createdAt"::date BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 8) AS orders_prior_30d,
        COUNT(*) FILTER (WHERE o."createdAt"::date >= CURRENT_DATE - 37)  AS orders_last_37d,
        MAX(o."createdAt"::date) AS last_order_date,
        SUM(o."totalPrice"::numeric) FILTER (WHERE o."createdAt"::date BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 8) AS revenue_prior_30d
    FROM raw_dash.orders o
    INNER JOIN raw_dash.customers c ON c."_id" = o."customer"
    WHERE LOWER(o."status") = 'delivered'
      AND c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY o."customer", {{ clean_business_name('c."businessName"') }}
)

SELECT
    customer_id,
    business_name,
    orders_this_week,
    orders_prior_30d,
    revenue_prior_30d,
    last_order_date,
    CURRENT_DATE - last_order_date AS days_since_last_order,

    CASE
        WHEN orders_prior_30d > 0 AND orders_this_week = 0
        THEN 'Churned'
        WHEN orders_prior_30d = 0 AND orders_this_week > 0
        THEN 'Reactivated'
        WHEN orders_prior_30d > 0 AND orders_this_week > 0
        THEN 'Active'
        ELSE 'Dormant'
    END AS weekly_status,

    CURRENT_DATE AS snapshot_date

FROM brand_orders
WHERE orders_prior_30d > 0 OR orders_this_week > 0
