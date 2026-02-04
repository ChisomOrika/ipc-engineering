{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}

WITH customer_activity AS (
    SELECT 
        o.unified_customer_id_fk AS customer_id,
        c.customer_businessname AS customer_name,
        o.receipts_createdat_date as order_date,
        COUNT(o.receipts_id_pk) AS total_orders, 
        
        -- Calculate repeat orders: excluding distinct months for the same order
        COUNT(o.receipts_id_pk) - COUNT(DISTINCT DATE_TRUNC('month', o.receipts_createdat_date)) AS repeat_orders,
        
        -- Max last order date where status is 'DELIVERED'
        MAX(CASE WHEN o.receipts_status IN ('DELIVERED', 'delivered') THEN o.receipts_createdat_date ELSE NULL END) AS last_order_date,
        
        -- Calculate if the customer is active based on the last order being within 6 weeks of current date
        CASE 
            WHEN MAX(CASE WHEN o.receipts_status IN ('DELIVERED', 'delivered') THEN o.receipts_createdat_date ELSE NULL END) >= CURRENT_DATE - INTERVAL '6 weeks' 
            THEN TRUE
            ELSE FALSE
        END AS is_active,
        
        -- Count orders where status is 'DELIVERED'
        COUNT(CASE WHEN o.receipts_status IN ('DELIVERED', 'delivered') THEN 1 END) AS orders_delivered,

         -- Count orders where status is 'DELIVERED'
        COUNT(CASE WHEN o.receipts_status IN ('CANCELLED', 'cancelled') THEN 1 END) AS orders_cancelled,
        
        -- Count orders from previous month
        COUNT(CASE WHEN DATE_TRUNC('month', o.receipts_createdat_date) = CURRENT_DATE - INTERVAL '1 month' THEN 1 END) AS orders_last_month,
        
        -- Count orders from two months ago
        COUNT(CASE WHEN DATE_TRUNC('month', o.receipts_createdat_date) = CURRENT_DATE - INTERVAL '2 months' THEN 1 END) AS orders_two_months_ago,
        
        -- Count orders from three months ago
        COUNT(CASE WHEN DATE_TRUNC('month', o.receipts_createdat_date) = CURRENT_DATE - INTERVAL '3 months' THEN 1 END) AS orders_three_months_ago

    FROM {{ ref('bv_customers') }} c  -- Reference the bv_customers model
    RIGHT JOIN {{ ref('bv_receipts') }} o  -- Reference the bv_receipts model
        ON c.customer_id_pk = o.unified_customer_id_fk
        
    GROUP BY o.unified_customer_id_fk, c.customer_businessname, o.receipts_createdat_date
)

SELECT * FROM customer_activity


