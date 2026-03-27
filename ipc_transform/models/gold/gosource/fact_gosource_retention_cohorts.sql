{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  Monthly signup cohorts with retention at M1, M3, M6, M12.
  GoSource version.
  A brand is "retained" in month N if they had at least 1 delivered order
  in the Nth month after their signup month.
*/

WITH brand_signup AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        MIN(c."createdAt"::date)  AS signup_date,
        DATE_TRUNC('month', MIN(c."createdAt"::date))::date AS cohort_month
    FROM raw_gosource.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY {{ clean_business_name('c."businessName"') }}
),

brand_ids AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        c."_id" AS customer_id
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

brand_order_months AS (
    SELECT DISTINCT
        bi.business_name,
        DATE_TRUNC('month', o."createdAt"::date)::date AS order_month
    FROM deduped_orders o
    INNER JOIN brand_ids bi ON bi.customer_id = COALESCE(o."business._id", o.business)
    WHERE LOWER(o.status) = 'delivered'
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM brand_signup
    GROUP BY cohort_month
),

retention_check AS (
    SELECT
        bs.business_name,
        bs.cohort_month,
        bs.signup_date,
        bom.order_month,
        -- months since signup
        (EXTRACT(YEAR FROM bom.order_month) - EXTRACT(YEAR FROM bs.cohort_month)) * 12
        + (EXTRACT(MONTH FROM bom.order_month) - EXTRACT(MONTH FROM bs.cohort_month)) AS months_since_signup
    FROM brand_signup bs
    INNER JOIN brand_order_months bom ON bom.business_name = bs.business_name
),

retention_flags AS (
    SELECT
        business_name,
        cohort_month,
        MAX(CASE WHEN months_since_signup >= 1 THEN 1 ELSE 0 END) AS retained_m1,
        MAX(CASE WHEN months_since_signup >= 3 THEN 1 ELSE 0 END) AS retained_m3,
        MAX(CASE WHEN months_since_signup >= 6 THEN 1 ELSE 0 END) AS retained_m6,
        MAX(CASE WHEN months_since_signup >= 12 THEN 1 ELSE 0 END) AS retained_m12,
        MAX(months_since_signup) AS max_months_active
    FROM retention_check
    GROUP BY business_name, cohort_month
)

-- Per-cohort aggregated retention rates
SELECT
    cs.cohort_month,
    cs.cohort_size,
    COALESCE(SUM(rf.retained_m1), 0) AS retained_m1,
    COALESCE(SUM(rf.retained_m3), 0) AS retained_m3,
    COALESCE(SUM(rf.retained_m6), 0) AS retained_m6,
    COALESCE(SUM(rf.retained_m12), 0) AS retained_m12,
    ROUND(COALESCE(SUM(rf.retained_m1), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1) AS retention_rate_m1,
    ROUND(COALESCE(SUM(rf.retained_m3), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1) AS retention_rate_m3,
    ROUND(COALESCE(SUM(rf.retained_m6), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1) AS retention_rate_m6,
    ROUND(COALESCE(SUM(rf.retained_m12), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1) AS retention_rate_m12,
    -- Only include retention rate if enough time has passed for the cohort
    CASE WHEN cs.cohort_month <= CURRENT_DATE - INTERVAL '1 month' THEN
        ROUND(COALESCE(SUM(rf.retained_m1), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1)
    END AS retention_rate_m1_valid,
    CASE WHEN cs.cohort_month <= CURRENT_DATE - INTERVAL '3 months' THEN
        ROUND(COALESCE(SUM(rf.retained_m3), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1)
    END AS retention_rate_m3_valid,
    CASE WHEN cs.cohort_month <= CURRENT_DATE - INTERVAL '6 months' THEN
        ROUND(COALESCE(SUM(rf.retained_m6), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1)
    END AS retention_rate_m6_valid,
    CASE WHEN cs.cohort_month <= CURRENT_DATE - INTERVAL '12 months' THEN
        ROUND(COALESCE(SUM(rf.retained_m12), 0)::numeric / NULLIF(cs.cohort_size, 0) * 100, 1)
    END AS retention_rate_m12_valid
FROM cohort_sizes cs
LEFT JOIN retention_flags rf ON rf.cohort_month = cs.cohort_month
GROUP BY cs.cohort_month, cs.cohort_size
ORDER BY cs.cohort_month
