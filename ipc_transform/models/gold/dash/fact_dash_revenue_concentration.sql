{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH + GoSource revenue concentration analysis.
  Shows how dependent each service line is on its top customers.
  Management-only — not for the operational dashboard.
  One row per customer per service line.
*/

WITH dash_revenue AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        'DAASH'            AS service_line,
        COUNT(*)           AS lifetime_orders,
        ROUND(SUM(o."totalPrice"::numeric)) AS lifetime_revenue,
        COUNT(*) FILTER (WHERE o."createdAt"::date >= CURRENT_DATE - 30) AS orders_last_30d,
        ROUND(SUM(CASE WHEN o."createdAt"::date >= CURRENT_DATE - 30 THEN o."totalPrice"::numeric ELSE 0 END)) AS revenue_last_30d
    FROM raw_dash.orders o
    JOIN raw_dash.customers c ON o."customer" = c."_id"
    WHERE LOWER(o."status") = 'delivered'
      AND c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY {{ clean_business_name('c."businessName"') }}
),

gosource_revenue AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        'GoSource'         AS service_line,
        COUNT(*)           AS lifetime_orders,
        ROUND(SUM(r."totalPrice"::numeric)) AS lifetime_revenue,
        COUNT(*) FILTER (WHERE r."createdAt"::date >= CURRENT_DATE - 30) AS orders_last_30d,
        ROUND(SUM(CASE WHEN r."createdAt"::date >= CURRENT_DATE - 30 THEN r."totalPrice"::numeric ELSE 0 END)) AS revenue_last_30d
    FROM raw_gosource.receipts r
    LEFT JOIN raw_gosource.branches b ON r."branch" = b."_id"
    JOIN raw_gosource.customers c ON COALESCE(r."customerId", r."business", b."businessId") = c."_id"
    WHERE LOWER(r."status") = 'delivered'
      AND c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY {{ clean_business_name('c."businessName"') }}
),

combined AS (
    SELECT * FROM dash_revenue
    UNION ALL
    SELECT * FROM gosource_revenue
),

with_totals AS (
    SELECT
        *,
        SUM(lifetime_revenue) OVER (PARTITION BY service_line) AS total_service_revenue,
        SUM(revenue_last_30d) OVER (PARTITION BY service_line) AS total_service_revenue_30d,
        ROW_NUMBER() OVER (PARTITION BY service_line ORDER BY lifetime_revenue DESC) AS revenue_rank
    FROM combined
)

SELECT
    business_name,
    service_line,
    lifetime_orders,
    lifetime_revenue,
    orders_last_30d,
    revenue_last_30d,
    revenue_rank,

    -- Concentration metrics
    ROUND(lifetime_revenue::numeric / NULLIF(total_service_revenue, 0) * 100, 1)
        AS pct_of_total_revenue,
    ROUND(revenue_last_30d::numeric / NULLIF(total_service_revenue_30d, 0) * 100, 1)
        AS pct_of_total_revenue_30d,

    -- Cumulative share (top N analysis)
    ROUND(SUM(lifetime_revenue) OVER (PARTITION BY service_line ORDER BY lifetime_revenue DESC)::numeric
          / NULLIF(total_service_revenue, 0) * 100, 1)
        AS cumulative_revenue_pct,

    CURRENT_DATE AS snapshot_date

FROM with_totals
ORDER BY service_line, revenue_rank