{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  Restaurant (brand) engagement health score.
  One row per DAASH customer (brand).

  Engagement signals (9 for web-enabled, 7 for POS-only):
    1. Order volume  — orders in last 30d vs prior 30d
    2. Website traffic — visit records in last 30d vs prior 30d  [web-only]
    3. Platform activity — activity log entries in last 30d vs prior 30d
    4. Menu freshness — days since last menu item update
    5. Staff adoption — active (non-deactivated) members count
    6. Delivery quality — % of deliveries completed vs total
    7. Subscription status — active subscription
    8. Channel adoption — any website orders in last 90d  [web-only]
    9. Order quality — rejection/void rate below 5%

  POS-only brands (0 website orders ever) are scored out of 7 — signals
  2 and 8 are excluded from their denominator. Thresholds scale:
    Web-enabled (out of 9): Healthy ≥7, At Risk ≥4, Critical <4
    POS-only    (out of 7): Healthy ≥5, At Risk ≥3, Critical <3
*/

-- Only include restaurants that have ever placed at least 1 order
WITH customers_with_orders AS (
    SELECT DISTINCT "customer" AS customer_id
    FROM raw_dash.orders
),

customers AS (
    SELECT
        c."_id"            AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name,
        c."email"          AS email,
        c."verified"       AS verified,
        c."createdAt"::date AS signup_date,
        CURRENT_DATE - c."createdAt"::date AS tenure_days
    FROM raw_dash.customers c
    INNER JOIN customers_with_orders cwo ON cwo.customer_id = c."_id"
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
),

-- 1. ORDER VOLUME
orders_current AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS orders_last_30d,
        SUM("totalPrice"::numeric) AS revenue_last_30d
    FROM raw_dash.orders
    WHERE "createdAt"::date >= CURRENT_DATE - 30
      AND LOWER("status") = 'delivered'
    GROUP BY "customer"
),
orders_prior AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS orders_prior_30d,
        SUM("totalPrice"::numeric) AS revenue_prior_30d
    FROM raw_dash.orders
    WHERE "createdAt"::date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
      AND LOWER("status") = 'delivered'
    GROUP BY "customer"
),
last_order AS (
    SELECT
        "customer"       AS customer_id,
        MAX("createdAt"::date) AS last_order_date
    FROM raw_dash.orders
    WHERE LOWER("status") = 'delivered'
    GROUP BY "customer"
),

-- 2. WEBSITE TRAFFIC
visits_current AS (
    SELECT
        "customer"       AS customer_id,
        COALESCE(SUM("visits"::int), 0) AS visits_last_30d
    FROM raw_dash.websitevisits
    WHERE "updatedAt"::date >= CURRENT_DATE - 30
    GROUP BY "customer"
),
visits_prior AS (
    SELECT
        "customer"       AS customer_id,
        COALESCE(SUM("visits"::int), 0) AS visits_prior_30d
    FROM raw_dash.websitevisits
    WHERE "updatedAt"::date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
    GROUP BY "customer"
),

-- 3. PLATFORM ACTIVITY (logins, menu edits, etc.)
activity_current AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS activity_last_30d
    FROM raw_dash.activitylogs
    WHERE "createdAt"::date >= CURRENT_DATE - 30
    GROUP BY "customer"
),
activity_prior AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS activity_prior_30d
    FROM raw_dash.activitylogs
    WHERE "createdAt"::date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
    GROUP BY "customer"
),
last_activity AS (
    SELECT
        "customer"       AS customer_id,
        MAX("createdAt"::date) AS last_activity_date
    FROM raw_dash.activitylogs
    GROUP BY "customer"
),

-- 4. MENU FRESHNESS
menu_freshness AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS total_menu_items,
        COUNT(*) FILTER (WHERE "active"::text = 'true')  AS active_menu_items,
        MAX("updatedAt"::date) AS last_menu_update
    FROM raw_dash.menuitems
    GROUP BY "customer"
),

-- 5. STAFF / MEMBERS
staff AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS total_members,
        COUNT(*) FILTER (WHERE COALESCE("isDeactivated"::text, 'false') != 'true') AS active_members
    FROM raw_dash.members
    GROUP BY "customer"
),

-- 6. DELIVERY QUALITY (last 90 days)
delivery_quality AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS total_deliveries,
        COUNT(*) FILTER (WHERE LOWER("status") = 'delivered')   AS completed_deliveries,
        COUNT(*) FILTER (WHERE LOWER("status") IN ('cancelled', 'failed')) AS failed_deliveries
    FROM raw_dash.deliveries
    WHERE "createdAt"::date >= CURRENT_DATE - 90
    GROUP BY "customer"
),

-- 7. SUBSCRIPTION STATUS (latest per customer)
subscriptions AS (
    SELECT DISTINCT ON ("customer")
        "customer"       AS customer_id,
        "isActive"       AS sub_is_active,
        "autoRenew"      AS sub_auto_renew,
        "plan"           AS sub_plan_id,
        "onTrial"        AS sub_on_trial,
        "updatedAt"::date AS sub_updated_at
    FROM raw_dash.subscriptions
    ORDER BY "customer", "updatedAt" DESC
),

-- 8. CHANNEL ADOPTION (website usage = higher engagement)
channel_mix AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS total_orders_all,
        COUNT(*) FILTER (WHERE "channel" = 'website') AS web_orders,
        ROUND(COUNT(*) FILTER (WHERE "channel" = 'website')::numeric
              / NULLIF(COUNT(*), 0) * 100, 1) AS web_pct
    FROM raw_dash.orders
    WHERE LOWER("status") = 'delivered'
      AND "createdAt"::date >= CURRENT_DATE - 90
    GROUP BY "customer"
),

-- POS-ONLY DETECTION (brands with zero website orders ever)
pos_only AS (
    SELECT
        "customer"       AS customer_id,
        CASE WHEN COALESCE(COUNT(*) FILTER (WHERE "channel" = 'website'), 0) = 0
             THEN TRUE ELSE FALSE
        END AS is_pos_only
    FROM raw_dash.orders
    WHERE LOWER("status") = 'delivered'
    GROUP BY "customer"
),

-- 9. ORDER QUALITY (rejection/void rate)
order_quality AS (
    SELECT
        "customer"       AS customer_id,
        COUNT(*)         AS total_orders_90d,
        COUNT(*) FILTER (WHERE LOWER("status") IN ('rejected', 'voided')) AS failed_orders_90d,
        ROUND(COUNT(*) FILTER (WHERE LOWER("status") IN ('rejected', 'voided'))::numeric
              / NULLIF(COUNT(*), 0) * 100, 1) AS fail_rate_pct
    FROM raw_dash.orders
    WHERE "createdAt"::date >= CURRENT_DATE - 90
    GROUP BY "customer"
),

-- BRANCHES PER CUSTOMER
branch_counts AS (
    SELECT
        "customerId"     AS customer_id,
        COUNT(*)         AS total_branches,
        COUNT(*) FILTER (WHERE "isActive"::text = 'true') AS active_branches
    FROM raw_dash.branches
    GROUP BY "customerId"
),

-- ASSEMBLE
assembled AS (
    SELECT
        c.customer_id,
        c.business_name,
        c.email,
        c.signup_date,
        c.tenure_days,

        -- Branches
        COALESCE(br.total_branches, 0)      AS total_branches,
        COALESCE(br.active_branches, 0)     AS active_branches,

        -- Orders
        COALESCE(oc.orders_last_30d, 0)     AS orders_last_30d,
        COALESCE(op.orders_prior_30d, 0)    AS orders_prior_30d,
        COALESCE(oc.revenue_last_30d, 0)    AS revenue_last_30d,
        COALESCE(op.revenue_prior_30d, 0)   AS revenue_prior_30d,
        lo.last_order_date,
        CURRENT_DATE - lo.last_order_date   AS days_since_last_order,

        -- Website visits
        COALESCE(vc.visits_last_30d, 0)     AS visits_last_30d,
        COALESCE(vp.visits_prior_30d, 0)    AS visits_prior_30d,

        -- Platform activity
        COALESCE(ac.activity_last_30d, 0)   AS activity_last_30d,
        COALESCE(ap.activity_prior_30d, 0)  AS activity_prior_30d,
        la.last_activity_date,
        CURRENT_DATE - la.last_activity_date AS days_since_last_activity,

        -- Menu
        COALESCE(mf.total_menu_items, 0)    AS total_menu_items,
        COALESCE(mf.active_menu_items, 0)   AS active_menu_items,
        mf.last_menu_update,
        CURRENT_DATE - mf.last_menu_update  AS days_since_menu_update,

        -- Staff
        COALESCE(st.total_members, 0)       AS total_members,
        COALESCE(st.active_members, 0)      AS active_members,

        -- Deliveries
        COALESCE(dq.total_deliveries, 0)    AS deliveries_last_90d,
        COALESCE(dq.completed_deliveries, 0) AS completed_deliveries_90d,
        COALESCE(dq.failed_deliveries, 0)   AS failed_deliveries_90d,
        CASE WHEN COALESCE(dq.total_deliveries, 0) > 0
             THEN ROUND(dq.completed_deliveries::numeric / dq.total_deliveries * 100, 1)
             ELSE NULL
        END AS delivery_completion_rate,

        -- Subscription
        sub.sub_is_active,
        sub.sub_auto_renew,
        sub.sub_on_trial,

        -- Channel adoption
        COALESCE(cm.web_orders, 0)         AS web_orders_90d,
        COALESCE(cm.web_pct, 0)            AS web_order_pct,

        -- Order quality
        COALESCE(oq.failed_orders_90d, 0)  AS failed_orders_90d,
        COALESCE(oq.fail_rate_pct, 0)      AS order_fail_rate_pct,

        -- POS-only flag
        COALESCE(po.is_pos_only, TRUE) AS is_pos_only,

        -- SIGNAL SCORES (1 = healthy, 0 = at risk)
        -- Signal 1: Order volume not declining >50%
        CASE WHEN COALESCE(oc.orders_last_30d, 0) > 0
              AND (op.orders_prior_30d IS NULL OR op.orders_prior_30d = 0
                   OR oc.orders_last_30d::numeric / op.orders_prior_30d >= 0.5)
             THEN 1 ELSE 0
        END AS signal_orders,

        -- Signal 2: Website visits not declining >50% (excluded for POS-only)
        CASE WHEN COALESCE(vc.visits_last_30d, 0) > 0
              AND (vp.visits_prior_30d IS NULL OR vp.visits_prior_30d = 0
                   OR vc.visits_last_30d::numeric / vp.visits_prior_30d >= 0.5)
             THEN 1 ELSE 0
        END AS signal_visits,

        -- Signal 3: Platform activity in last 14 days
        CASE WHEN la.last_activity_date >= CURRENT_DATE - 14
             THEN 1 ELSE 0
        END AS signal_activity,

        -- Signal 4: Menu updated in last 60 days
        CASE WHEN mf.last_menu_update >= CURRENT_DATE - 60
             THEN 1 ELSE 0
        END AS signal_menu,

        -- Signal 5: 2+ active staff members
        CASE WHEN COALESCE(st.active_members, 0) >= 2
             THEN 1 ELSE 0
        END AS signal_staff,

        -- Signal 6: Delivery completion rate > 80%
        CASE WHEN COALESCE(dq.total_deliveries, 0) > 0
              AND dq.completed_deliveries::numeric / dq.total_deliveries >= 0.8
             THEN 1 ELSE 0
        END AS signal_delivery,

        -- Signal 7: Subscription active
        CASE WHEN sub.sub_is_active::text = 'true'
             THEN 1 ELSE 0
        END AS signal_subscription,

        -- Signal 8: Website channel adoption (any web orders in 90d)
        CASE WHEN COALESCE(cm.web_orders, 0) > 0
             THEN 1 ELSE 0
        END AS signal_channel,

        -- Signal 9: Low rejection/void rate (< 5%) — must have orders in 90d
        CASE WHEN COALESCE(oq.total_orders_90d, 0) > 0
              AND COALESCE(oq.fail_rate_pct, 0) < 5
             THEN 1 ELSE 0
        END AS signal_quality

    FROM customers c
    LEFT JOIN branch_counts br       ON br.customer_id = c.customer_id
    LEFT JOIN orders_current oc      ON oc.customer_id = c.customer_id
    LEFT JOIN orders_prior op        ON op.customer_id = c.customer_id
    LEFT JOIN last_order lo          ON lo.customer_id = c.customer_id
    LEFT JOIN visits_current vc      ON vc.customer_id = c.customer_id
    LEFT JOIN visits_prior vp        ON vp.customer_id = c.customer_id
    LEFT JOIN activity_current ac    ON ac.customer_id = c.customer_id
    LEFT JOIN activity_prior ap      ON ap.customer_id = c.customer_id
    LEFT JOIN last_activity la       ON la.customer_id = c.customer_id
    LEFT JOIN menu_freshness mf      ON mf.customer_id = c.customer_id
    LEFT JOIN staff st               ON st.customer_id = c.customer_id
    LEFT JOIN delivery_quality dq    ON dq.customer_id = c.customer_id
    LEFT JOIN subscriptions sub      ON sub.customer_id = c.customer_id
    LEFT JOIN channel_mix cm         ON cm.customer_id = c.customer_id
    LEFT JOIN pos_only po            ON po.customer_id = c.customer_id
    LEFT JOIN order_quality oq       ON oq.customer_id = c.customer_id
)

SELECT
    *,

    -- Score max: 7 for POS-only (exclude signal_visits + signal_channel), 9 for web-enabled
    CASE WHEN is_pos_only THEN 7 ELSE 9 END AS score_max,

    -- Total health score — POS-only brands: exclude signal_visits and signal_channel
    CASE WHEN is_pos_only
         THEN signal_orders + signal_activity + signal_menu
              + signal_staff + signal_delivery + signal_subscription
              + signal_quality
         ELSE signal_orders + signal_visits + signal_activity + signal_menu
              + signal_staff + signal_delivery + signal_subscription
              + signal_channel + signal_quality
    END AS health_score,

    -- Health status (proportional thresholds)
    -- Web-enabled (9): Healthy ≥7, At Risk ≥4
    -- POS-only   (7): Healthy ≥5, At Risk ≥3
    CASE
        WHEN is_pos_only THEN
            CASE
                WHEN signal_orders + signal_activity + signal_menu
                     + signal_staff + signal_delivery + signal_subscription
                     + signal_quality >= 5
                THEN 'Healthy'
                WHEN signal_orders + signal_activity + signal_menu
                     + signal_staff + signal_delivery + signal_subscription
                     + signal_quality >= 3
                THEN 'At Risk'
                ELSE 'Critical'
            END
        ELSE
            CASE
                WHEN signal_orders + signal_visits + signal_activity + signal_menu
                     + signal_staff + signal_delivery + signal_subscription
                     + signal_channel + signal_quality >= 7
                THEN 'Healthy'
                WHEN signal_orders + signal_visits + signal_activity + signal_menu
                     + signal_staff + signal_delivery + signal_subscription
                     + signal_channel + signal_quality >= 4
                THEN 'At Risk'
                ELSE 'Critical'
            END
    END AS health_status,

    -- Order volume change %
    CASE WHEN COALESCE(orders_prior_30d, 0) > 0
         THEN ROUND((orders_last_30d - orders_prior_30d)::numeric / orders_prior_30d * 100, 1)
         ELSE NULL
    END AS order_volume_change_pct,

    -- Revenue change %
    CASE WHEN COALESCE(revenue_prior_30d, 0) > 0
         THEN ROUND((revenue_last_30d - revenue_prior_30d)::numeric / revenue_prior_30d * 100, 1)
         ELSE NULL
    END AS revenue_change_pct,

    -- Visit change %
    CASE WHEN COALESCE(visits_prior_30d, 0) > 0
         THEN ROUND((visits_last_30d - visits_prior_30d)::numeric / visits_prior_30d * 100, 1)
         ELSE NULL
    END AS visit_change_pct,

    -- Activity change %
    CASE WHEN COALESCE(activity_prior_30d, 0) > 0
         THEN ROUND((activity_last_30d - activity_prior_30d)::numeric / activity_prior_30d * 100, 1)
         ELSE NULL
    END AS activity_change_pct,

    CURRENT_DATE AS snapshot_date

FROM assembled