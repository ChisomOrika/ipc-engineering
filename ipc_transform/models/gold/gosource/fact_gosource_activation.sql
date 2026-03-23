{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  GoSource brand activation analysis.
  One row per brand (deduplicated by cleaned business_name) — measures how
  quickly they received their first user order after signup.

  Uses the same dual customer resolution as dim_gosource_customer_health:
    COALESCE(r."customerId", r."business", b."businessId")
*/

WITH receipt_customer AS (
    SELECT
        r."_id" AS receipt_id,
        r."createdAt"::date AS order_date,
        COALESCE(
            r."customerId",
            r."business",
            b."businessId"
        ) AS customer_id
    FROM raw_gosource.receipts r
    LEFT JOIN raw_gosource.branches b ON r."branch" = b."_id"
    WHERE LOWER(r."status") = 'delivered'
),

customers AS (
    SELECT
        c."_id"              AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name,
        c."createdAt"::date  AS signup_date
    FROM raw_gosource.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
),

-- Deduplicate: one row per brand, earliest signup
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
        MIN(rc.order_date) AS first_order_date,
        MAX(rc.order_date) AS last_order_date,
        COUNT(*)           AS lifetime_orders
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON rc.customer_id = bi.customer_id
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
