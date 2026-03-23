{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH brand activation analysis.
  One row per brand (deduplicated by cleaned business_name) — measures how
  quickly they received their first user order after signup.

  Key metrics:
    - days_to_first_order: calendar days from signup to first delivered order
    - activated_7d / 14d / 30d / 60d: boolean flags for activation window
    - days_since_last_order: staleness indicator
    - activation_status: Never Activated / Quick Start / Slow Start / Dormant
*/

WITH customers AS (
    SELECT
        c."_id"              AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name,
        c."createdAt"::date  AS signup_date
    FROM raw_dash.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
),

-- Aggregate across all customer_ids that share the same cleaned brand name
brand_signup AS (
    SELECT
        business_name,
        MIN(signup_date) AS signup_date
    FROM customers
    GROUP BY business_name
),

-- Collect all customer_ids per brand for order matching
brand_ids AS (
    SELECT business_name, customer_id
    FROM customers
),

order_stats AS (
    SELECT
        bi.business_name,
        MIN(o."createdAt"::date) AS first_order_date,
        MAX(o."createdAt"::date) AS last_order_date,
        COUNT(*)                 AS lifetime_orders
    FROM raw_dash.orders o
    INNER JOIN brand_ids bi ON o."customer" = bi.customer_id
    WHERE LOWER(o."status") = 'delivered'
    GROUP BY bi.business_name
)

SELECT
    b.business_name,
    b.signup_date,
    CURRENT_DATE - b.signup_date AS tenure_days,

    o.first_order_date,
    o.last_order_date,
    COALESCE(o.lifetime_orders, 0) AS lifetime_orders,

    CASE WHEN o.first_order_date IS NOT NULL
         THEN o.first_order_date - b.signup_date
         ELSE NULL
    END AS days_to_first_order,

    CASE WHEN o.last_order_date IS NOT NULL
         THEN CURRENT_DATE - o.last_order_date
         ELSE NULL
    END AS days_since_last_order,

    CASE WHEN o.first_order_date IS NOT NULL
          AND o.first_order_date - b.signup_date <= 7
         THEN TRUE ELSE FALSE
    END AS activated_7d,

    CASE WHEN o.first_order_date IS NOT NULL
          AND o.first_order_date - b.signup_date <= 14
         THEN TRUE ELSE FALSE
    END AS activated_14d,

    CASE WHEN o.first_order_date IS NOT NULL
          AND o.first_order_date - b.signup_date <= 30
         THEN TRUE ELSE FALSE
    END AS activated_30d,

    CASE WHEN o.first_order_date IS NOT NULL
          AND o.first_order_date - b.signup_date <= 60
         THEN TRUE ELSE FALSE
    END AS activated_60d,

    CASE
        WHEN o.first_order_date IS NULL
            THEN 'Never Activated'
        WHEN o.first_order_date - b.signup_date <= 7
            THEN 'Quick Start (≤7d)'
        WHEN o.first_order_date - b.signup_date <= 30
            THEN 'Normal Start (8-30d)'
        WHEN o.first_order_date - b.signup_date <= 60
            THEN 'Slow Start (31-60d)'
        ELSE 'Very Slow (60d+)'
    END AS activation_status,

    CASE
        WHEN o.first_order_date IS NULL
            THEN 'Never Ordered'
        WHEN CURRENT_DATE - o.last_order_date <= 30
            THEN 'Active'
        WHEN CURRENT_DATE - o.last_order_date <= 60
            THEN 'Cooling Off'
        WHEN CURRENT_DATE - o.last_order_date <= 90
            THEN 'At Risk'
        ELSE 'Dormant'
    END AS engagement_status,

    CURRENT_DATE AS snapshot_date

FROM brand_signup b
LEFT JOIN order_stats o ON o.business_name = b.business_name
ORDER BY b.signup_date DESC
