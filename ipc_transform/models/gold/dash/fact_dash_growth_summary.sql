{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  Weekly growth summary — new brands, churned, reactivated, net growth.
  One row per week (Monday-based).
  Also tracks adoption metrics: POS-only vs web-enabled, platform logins.
*/

WITH brand_signup AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        MIN(c."createdAt"::date) AS signup_date,
        ARRAY_AGG(DISTINCT c."_id") AS customer_ids
    FROM raw_dash.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY {{ clean_business_name('c."businessName"') }}
),

-- Generate weekly buckets
weeks AS (
    SELECT generate_series(
        DATE_TRUNC('week', (SELECT MIN(signup_date) FROM brand_signup))::date,
        DATE_TRUNC('week', CURRENT_DATE)::date,
        '1 week'::interval
    )::date AS week_start
),

-- New brands per week (first signup falls in this week)
new_brands AS (
    SELECT
        DATE_TRUNC('week', signup_date)::date AS week_start,
        COUNT(*) AS new_brand_count
    FROM brand_signup
    GROUP BY DATE_TRUNC('week', signup_date)::date
),

-- Brand order activity per week
brand_weekly_orders AS (
    SELECT
        bs.business_name,
        DATE_TRUNC('week', o."createdAt"::date)::date AS week_start,
        COUNT(*) AS order_count,
        COUNT(*) FILTER (WHERE o."channel" = 'website') AS web_orders
    FROM raw_dash.orders o
    INNER JOIN raw_dash.customers c ON c."_id" = o."customer"
    INNER JOIN brand_signup bs ON bs.business_name = {{ clean_business_name('c."businessName"') }}
    WHERE LOWER(o."status") = 'delivered'
    GROUP BY bs.business_name, DATE_TRUNC('week', o."createdAt"::date)::date
),

-- Active brands per week (had at least 1 order)
active_per_week AS (
    SELECT
        week_start,
        COUNT(DISTINCT business_name) AS active_brands,
        COUNT(DISTINCT business_name) FILTER (WHERE web_orders > 0) AS web_enabled_brands,
        COUNT(DISTINCT business_name) FILTER (WHERE web_orders = 0) AS pos_only_brands,
        SUM(order_count) AS total_orders
    FROM brand_weekly_orders
    GROUP BY week_start
),

-- Churn detection: active last week but not this week
brand_week_pairs AS (
    SELECT DISTINCT business_name, week_start
    FROM brand_weekly_orders
),

churned_per_week AS (
    SELECT
        w.week_start,
        COUNT(*) AS churned_brands
    FROM weeks w
    INNER JOIN brand_week_pairs prev ON prev.week_start = w.week_start - INTERVAL '1 week'
    LEFT JOIN brand_week_pairs curr ON curr.business_name = prev.business_name AND curr.week_start = w.week_start
    WHERE curr.business_name IS NULL
    GROUP BY w.week_start
),

-- Reactivation: not active last week, active this week, but was active before
reactivated_per_week AS (
    SELECT
        w.week_start,
        COUNT(*) AS reactivated_brands
    FROM weeks w
    INNER JOIN brand_week_pairs curr ON curr.week_start = w.week_start
    LEFT JOIN brand_week_pairs prev ON prev.business_name = curr.business_name AND prev.week_start = w.week_start - INTERVAL '1 week'
    WHERE prev.business_name IS NULL
      -- Must have been active in some earlier week (not brand new this week)
      AND EXISTS (
          SELECT 1 FROM brand_week_pairs older
          WHERE older.business_name = curr.business_name
            AND older.week_start < w.week_start - INTERVAL '1 week'
      )
    GROUP BY w.week_start
)

SELECT
    w.week_start,
    COALESCE(nb.new_brand_count, 0)       AS new_brands,
    COALESCE(ap.active_brands, 0)         AS active_brands,
    COALESCE(ch.churned_brands, 0)        AS churned_brands,
    COALESCE(re.reactivated_brands, 0)    AS reactivated_brands,
    COALESCE(nb.new_brand_count, 0) - COALESCE(ch.churned_brands, 0) + COALESCE(re.reactivated_brands, 0)
        AS net_growth,
    COALESCE(ap.total_orders, 0)          AS total_orders,
    COALESCE(ap.web_enabled_brands, 0)    AS web_enabled_brands,
    COALESCE(ap.pos_only_brands, 0)       AS pos_only_brands,
    CASE WHEN COALESCE(ap.active_brands, 0) > 0
         THEN ROUND(ap.web_enabled_brands::numeric / ap.active_brands * 100, 1)
         ELSE 0
    END AS web_adoption_pct

FROM weeks w
LEFT JOIN new_brands nb          ON nb.week_start = w.week_start
LEFT JOIN active_per_week ap     ON ap.week_start = w.week_start
LEFT JOIN churned_per_week ch    ON ch.week_start = w.week_start
LEFT JOIN reactivated_per_week re ON re.week_start = w.week_start
ORDER BY w.week_start
