{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH subscription lifecycle per restaurant.
  Tracks subscription status, plan, trial usage, and churn signals.
  One row per customer (latest subscription state).
*/

WITH latest_sub AS (
    SELECT DISTINCT ON ("customer")
        "customer"            AS customer_id,
        "_id"                 AS subscription_id,
        "plan"                AS plan_id,
        "amount"::numeric     AS amount,
        "planDuration"        AS plan_duration,
        "isActive"            AS is_active,
        "onTrial"             AS on_trial,
        "remainingTrialDays"  AS remaining_trial_days,
        "isFirstTime"         AS is_first_time,
        "autoRenew"           AS auto_renew,
        "startDate"::date     AS start_date,
        "endDate"::date       AS end_date,
        CASE WHEN "cancelledAt" IS NOT NULL
             THEN TO_TIMESTAMP("cancelledAt"::numeric / 1000)::date
             ELSE NULL
        END AS cancelled_at,
        "createdAt"::date     AS created_at,
        "updatedAt"::date     AS updated_at
    FROM raw_dash.subscriptions
    ORDER BY "customer", "updatedAt" DESC
),

-- Count subscription records per customer (plan changes)
sub_history AS (
    SELECT
        "customer" AS customer_id,
        COUNT(*)   AS total_subscription_records,
        COUNT(DISTINCT "plan") AS distinct_plans_used
    FROM raw_dash.subscriptions
    GROUP BY "customer"
),

-- Order activity to contextualize subscription value
order_summary AS (
    SELECT
        "customer" AS customer_id,
        COUNT(*) AS lifetime_orders,
        MAX("createdAt"::date) AS last_order_date
    FROM raw_dash.orders
    WHERE LOWER("status") = 'delivered'
    GROUP BY "customer"
)

SELECT
    c."_id"              AS customer_id,
    {{ clean_business_name('c."businessName"') }} AS business_name,
    c."createdAt"::date  AS signup_date,

    -- Subscription state
    ls.is_active,
    ls.on_trial,
    ls.auto_renew,
    ls.amount            AS subscription_amount,
    ls.plan_duration,
    ls.start_date,
    ls.end_date,
    ls.cancelled_at,
    ls.updated_at        AS sub_last_updated,

    -- Subscription history
    COALESCE(sh.total_subscription_records, 0) AS total_sub_records,
    COALESCE(sh.distinct_plans_used, 0)        AS distinct_plans,

    -- Derived status
    CASE
        WHEN ls.subscription_id IS NULL THEN 'Never Subscribed'
        WHEN ls.cancelled_at IS NOT NULL THEN 'Cancelled'
        WHEN ls.is_active::text = 'true' AND ls.on_trial::text = 'true' THEN 'Active Trial'
        WHEN ls.is_active::text = 'true' THEN 'Active Paid'
        ELSE 'Expired'
    END AS subscription_status,

    -- Activity context
    COALESCE(os.lifetime_orders, 0) AS lifetime_orders,
    os.last_order_date,
    CURRENT_DATE - os.last_order_date AS days_since_last_order,

    -- Risk flags
    CASE WHEN ls.is_active::text != 'true' AND COALESCE(os.lifetime_orders, 0) > 100
         THEN TRUE ELSE FALSE
    END AS high_value_churned,

    CASE WHEN ls.is_active::text = 'true' AND ls.auto_renew::text != 'true'
         THEN TRUE ELSE FALSE
    END AS active_no_autorenew

FROM raw_dash.customers c
LEFT JOIN latest_sub ls     ON ls.customer_id = c."_id"
LEFT JOIN sub_history sh    ON sh.customer_id = c."_id"
LEFT JOIN order_summary os  ON os.customer_id = c."_id"
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
  AND COALESCE(os.lifetime_orders, 0) > 0
ORDER BY os.lifetime_orders DESC