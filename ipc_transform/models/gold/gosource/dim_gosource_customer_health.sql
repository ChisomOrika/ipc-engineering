{{ config(
    materialized='table',
    schema='gold',
    tags=['GoSource']
) }}

/*
  GoSource B2B customer engagement health score.
  One row per GoSource business (deduplicated by cleaned business name).

  Customer resolution: receipts link to customers via two paths:
    - Old orders: receipts.customerId → customers._id
    - New orders: receipts.branch → branches._id → branches.businessId → customers._id
    - Also:       receipts.business → customers._id

  NOTE: inventorymovements and purchaseorders are GoSource internal
  operations (admin-created, no customer linkage), NOT customer signals.

  Engagement signals (6 customer-facing):
    1. Order volume    — orders in last 30d, not declining >50% vs prior 30d
    2. Order recency   — last order within 60 days
    3. Staff adoption  — 2+ active (non-deactivated) employees
    4. Branch presence  — at least 1 active branch
    5. Payment health  — >60% of credit orders paid (credit-only, all-time)
    6. Credit risk     — no overdue (90d+) unpaid orders

  Health status:
    'Healthy'   — 5-6 signals green
    'At Risk'   — 3-4 signals green
    'Critical'  — 0-2 signals green
*/

-- Resolve receipt → customer through all available paths
WITH receipt_customer AS (
    SELECT
        r."_id" AS receipt_id,
        r."totalPrice",
        r."status",
        r."createdAt",
        r."paymentMethod",
        r."paymentStatus",
        COALESCE(
            r."customerId",
            r."business",
            b."businessId"
        ) AS customer_id
    FROM raw_gosource.receipts r
    LEFT JOIN raw_gosource.branches b ON r."branch" = b."_id"
),

-- Deduplicate customers by cleaned business name
-- Multiple customer_ids can map to the same brand
brand_signup AS (
    SELECT
        {{ clean_business_name('c."businessName"') }} AS business_name,
        MIN(c."createdAt"::date) AS signup_date,
        MIN(c."email") AS email,
        BOOL_OR(c."verified"::boolean) AS verified,
        BOOL_OR(c."active"::boolean) AS active,
        BOOL_OR(COALESCE(c."canBuyOnCredit"::boolean, false)) AS can_buy_on_credit
    FROM raw_gosource.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
    GROUP BY {{ clean_business_name('c."businessName"') }}
),

-- Map all customer_ids to their cleaned brand name
brand_ids AS (
    SELECT
        c."_id" AS customer_id,
        {{ clean_business_name('c."businessName"') }} AS business_name
    FROM raw_gosource.customers c
    WHERE c."businessName" IS NOT NULL
      AND TRIM(c."businessName") != ''
      AND NOT {{ is_test_account('c."businessName"') }}
),

-- Only include brands that have ever placed at least 1 order
brands_with_orders AS (
    SELECT DISTINCT bi.business_name
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON bi.customer_id = rc.customer_id
    WHERE rc.customer_id IS NOT NULL
),

customers AS (
    SELECT
        bs.business_name,
        bs.email,
        bs.verified,
        bs.active,
        bs.can_buy_on_credit,
        bs.signup_date,
        CURRENT_DATE - bs.signup_date AS tenure_days
    FROM brand_signup bs
    INNER JOIN brands_with_orders bwo ON bwo.business_name = bs.business_name
),

-- 1. ORDER VOLUME (aggregated across all customer_ids for the brand)
orders_current AS (
    SELECT
        bi.business_name,
        COUNT(*)         AS orders_last_30d,
        SUM(rc."totalPrice"::numeric) AS revenue_last_30d
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON bi.customer_id = rc.customer_id
    WHERE rc."createdAt"::date >= CURRENT_DATE - 30
      AND LOWER(rc."status") = 'delivered'
    GROUP BY bi.business_name
),
orders_prior AS (
    SELECT
        bi.business_name,
        COUNT(*)         AS orders_prior_30d,
        SUM(rc."totalPrice"::numeric) AS revenue_prior_30d
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON bi.customer_id = rc.customer_id
    WHERE rc."createdAt"::date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
      AND LOWER(rc."status") = 'delivered'
    GROUP BY bi.business_name
),
last_order AS (
    SELECT
        bi.business_name,
        MAX(rc."createdAt"::date) AS last_order_date
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON bi.customer_id = rc.customer_id
    WHERE LOWER(rc."status") = 'delivered'
    GROUP BY bi.business_name
),

-- 2. STAFF / EMPLOYEES (aggregated across all customer_ids)
staff AS (
    SELECT
        bi.business_name,
        COUNT(*)         AS total_employees,
        COUNT(*) FILTER (WHERE COALESCE(e."isDeactivated"::text, 'false') != 'true') AS active_employees
    FROM raw_gosource.employees e
    INNER JOIN brand_ids bi ON bi.customer_id = e."businessId"
    GROUP BY bi.business_name
),

-- 3. BRANCHES (aggregated)
branch_counts AS (
    SELECT
        bi.business_name,
        COUNT(*)         AS total_branches,
        COUNT(*) FILTER (WHERE COALESCE(br."isDeactivated"::text, 'false') != 'true') AS active_branches
    FROM raw_gosource.branches br
    INNER JOIN brand_ids bi ON bi.customer_id = br."businessId"
    GROUP BY bi.business_name
),

-- 4. CREDIT AGING (overdue unpaid orders)
credit_aging AS (
    SELECT
        bi.business_name,
        COUNT(*) FILTER (WHERE CURRENT_DATE - rc."createdAt"::date > 90
                           AND LOWER(COALESCE(rc."paymentStatus", '')) != 'paid') AS overdue_90d_orders,
        COALESCE(SUM(CASE WHEN CURRENT_DATE - rc."createdAt"::date > 90
                           AND LOWER(COALESCE(rc."paymentStatus", '')) != 'paid'
                      THEN rc."totalPrice"::numeric ELSE 0 END), 0) AS overdue_90d_amount
    FROM receipt_customer rc
    INNER JOIN brand_ids bi ON bi.customer_id = rc.customer_id
    WHERE LOWER(rc."status") = 'delivered'
    GROUP BY bi.business_name
),

-- 5. PAYMENT HEALTH (credit orders only, all-time — measures collection reliability)
--    Transfer/Paystack orders are paid upfront so including them inflates the rate.
--    Customers who never used credit get NULL (not applicable).
payment_health AS (
    SELECT
        bi.business_name,
        COUNT(*) AS total_credit_orders,
        COUNT(*) FILTER (WHERE LOWER(COALESCE(o."paymentStatus", '')) = 'paid') AS paid_credit_orders
    FROM (
        SELECT DISTINCT ON (_id) *
        FROM raw_gosource.orders
        ORDER BY _id, "updatedAt" DESC
    ) o
    INNER JOIN brand_ids bi ON bi.customer_id = COALESCE(o."business._id", o.business)
    WHERE LOWER(o.status) = 'delivered'
      AND LOWER(o."paymentMethod") = 'credit'
    GROUP BY bi.business_name
),

-- ASSEMBLE
assembled AS (
    SELECT
        c.business_name,
        c.email,
        c.signup_date,
        c.tenure_days,
        c.can_buy_on_credit,

        -- Branches
        COALESCE(br.total_branches, 0)     AS total_branches,
        COALESCE(br.active_branches, 0)    AS active_branches,

        -- Orders
        COALESCE(oc.orders_last_30d, 0)    AS orders_last_30d,
        COALESCE(op.orders_prior_30d, 0)   AS orders_prior_30d,
        COALESCE(oc.revenue_last_30d, 0)   AS revenue_last_30d,
        COALESCE(op.revenue_prior_30d, 0)  AS revenue_prior_30d,
        lo.last_order_date,
        CURRENT_DATE - lo.last_order_date  AS days_since_last_order,

        -- Staff
        COALESCE(st.total_employees, 0)    AS total_employees,
        COALESCE(st.active_employees, 0)   AS active_employees,

        -- Payment health (credit orders only, all-time)
        COALESCE(ph.total_credit_orders, 0)  AS total_credit_orders,
        COALESCE(ph.paid_credit_orders, 0)   AS paid_credit_orders,
        CASE WHEN COALESCE(ph.total_credit_orders, 0) > 0
             THEN ROUND(ph.paid_credit_orders::numeric / ph.total_credit_orders * 100, 1)
             ELSE NULL
        END AS payment_rate,

        -- Credit aging
        COALESCE(ca.overdue_90d_orders, 0) AS overdue_90d_orders,
        COALESCE(ca.overdue_90d_amount, 0) AS overdue_90d_amount,

        -- SIGNAL SCORES (1 = healthy, 0 = at risk)

        -- Signal 1: Order volume — has orders and not declining >50%
        CASE WHEN COALESCE(oc.orders_last_30d, 0) > 0
              AND (op.orders_prior_30d IS NULL OR op.orders_prior_30d = 0
                   OR oc.orders_last_30d::numeric / op.orders_prior_30d >= 0.5)
             THEN 1 ELSE 0
        END AS signal_orders,

        -- Signal 2: Order recency — last order within 60 days
        CASE WHEN lo.last_order_date >= CURRENT_DATE - 60
             THEN 1 ELSE 0
        END AS signal_recency,

        -- Signal 3: Staff adoption — 2+ active employees
        CASE WHEN COALESCE(st.active_employees, 0) >= 2
             THEN 1 ELSE 0
        END AS signal_staff,

        -- Signal 4: Branch presence — at least 1 active branch
        CASE WHEN COALESCE(br.active_branches, 0) >= 1
             THEN 1 ELSE 0
        END AS signal_branches,

        -- Signal 5: Payment health — >60% of credit orders paid (or no credit orders)
        CASE WHEN COALESCE(ph.total_credit_orders, 0) = 0
             THEN 1  -- no credit usage = no payment risk
             WHEN ph.paid_credit_orders::numeric / ph.total_credit_orders >= 0.6
             THEN 1 ELSE 0
        END AS signal_payment,

        -- Signal 6: No overdue credit (no 90d+ unpaid orders)
        CASE WHEN COALESCE(ca.overdue_90d_orders, 0) = 0
             THEN 1 ELSE 0
        END AS signal_credit

    FROM customers c
    LEFT JOIN branch_counts br       ON br.business_name = c.business_name
    LEFT JOIN orders_current oc      ON oc.business_name = c.business_name
    LEFT JOIN orders_prior op        ON op.business_name = c.business_name
    LEFT JOIN last_order lo          ON lo.business_name = c.business_name
    LEFT JOIN staff st               ON st.business_name = c.business_name
    LEFT JOIN payment_health ph      ON ph.business_name = c.business_name
    LEFT JOIN credit_aging ca        ON ca.business_name = c.business_name
)

SELECT
    *,

    -- Total health score (out of 6)
    signal_orders + signal_recency + signal_staff
        + signal_branches + signal_payment + signal_credit
        AS health_score,

    6 AS score_max,

    -- Health status
    CASE
        WHEN signal_orders + signal_recency + signal_staff
             + signal_branches + signal_payment + signal_credit >= 5
        THEN 'Healthy'
        WHEN signal_orders + signal_recency + signal_staff
             + signal_branches + signal_payment + signal_credit >= 3
        THEN 'At Risk'
        ELSE 'Critical'
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

    CURRENT_DATE AS snapshot_date

FROM assembled
