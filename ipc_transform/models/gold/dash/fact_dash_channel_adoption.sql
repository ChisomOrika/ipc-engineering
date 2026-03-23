{{ config(
    materialized='table',
    schema='gold',
    tags=['DAASH']
) }}

/*
  DAASH channel adoption per restaurant.
  Shows POS vs Website usage — restaurants only using POS
  are under-utilizing the platform (upsell opportunity).
  One row per customer per month.
*/

WITH delivered AS (
    SELECT
        o."customer"       AS customer_id,
        DATE_TRUNC('month', o."createdAt"::date)::date AS order_month,
        o."channel",
        o."orderType",
        o."deliveryType",
        o."totalPrice"::numeric AS total_price
    FROM raw_dash.orders o
    WHERE LOWER(o."status") = 'delivered'
      AND o."customer" IS NOT NULL
),

monthly AS (
    SELECT
        d.customer_id,
        d.order_month,
        COUNT(*)                                              AS total_orders,
        COUNT(*) FILTER (WHERE d.channel = 'pos')             AS pos_orders,
        COUNT(*) FILTER (WHERE d.channel = 'website')         AS web_orders,
        COUNT(*) FILTER (WHERE d."deliveryType" = 'delivery') AS delivery_orders,
        COUNT(*) FILTER (WHERE d."deliveryType" = 'pickup')   AS pickup_orders,
        COUNT(*) FILTER (WHERE d."deliveryType" = 'eat_in')   AS eatin_orders,
        ROUND(COUNT(*) FILTER (WHERE d.channel = 'website')::numeric
              / NULLIF(COUNT(*), 0) * 100, 1)                 AS web_pct,
        ROUND(COUNT(*) FILTER (WHERE d."deliveryType" = 'delivery')::numeric
              / NULLIF(COUNT(*), 0) * 100, 1)                 AS delivery_pct
    FROM delivered d
    GROUP BY d.customer_id, d.order_month
)

SELECT
    m.*,
    {{ clean_business_name('c."businessName"') }} AS business_name
FROM monthly m
JOIN raw_dash.customers c ON c."_id" = m.customer_id
WHERE c."businessName" IS NOT NULL
  AND TRIM(c."businessName") != ''
  AND NOT {{ is_test_account('c."businessName"') }}
ORDER BY m.customer_id, m.order_month