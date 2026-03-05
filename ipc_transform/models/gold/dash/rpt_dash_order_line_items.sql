{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key=['order_reference', 'product_id']
) }}

SELECT
    o.order_reference AS order_reference,
    o.order_customer_id_fk AS customer,
    c.customer_business_name AS business_name,
    COALESCE(NULLIF(TRIM(CONCAT(u.user_first_name, ' ', u.user_last_name)), ''), 'Guest') AS name,
    o.order_payment_status AS payment_status,
    CASE
        WHEN o.order_created_at_date_time::DATE = CURRENT_DATE                        THEN 'Today'
        WHEN o.order_created_at_date_time::DATE = CURRENT_DATE - INTERVAL '1 day'    THEN 'Yesterday'
        ELSE TO_CHAR(o.order_created_at_date_time, 'Mon DD, YYYY')
    END                                                                AS created_date,
    o.order_type AS channel,
    o.order_delivery_type AS delivery_type,
    o.order_status AS status,
    o.order_payment_method AS mode_of_payment,
    o.order_updated_at_date_time AS order_updated_date_time,

    -- Exploded product fields
    prod->>'product'                                                   AS product_id,
    prod->'productDetails'->>'name'                                    AS product_name,
    (prod->>'quantity')::INT                                           AS quantity,
    (prod->>'price')::NUMERIC                                          AS price,
    (prod->>'quantity')::INT * (prod->>'price')::NUMERIC               AS amount,
    prod->'productDetails'->>'discounts'                               AS discount

FROM {{ ref('bv_dash_orders') }} o
LEFT JOIN {{ ref('bv_dash_users') }}    u ON o.order_user_id_fk     = u.user_id_pk
LEFT JOIN {{ ref('bv_dash_customers') }} c ON o.order_customer_id_fk = c.customer_id_pk
CROSS JOIN LATERAL jsonb_array_elements(o.order_products::jsonb) AS prod

{% if is_incremental() %}
  WHERE o.order_updated_at_date_time > (SELECT MAX(order_updated_date_time) FROM {{ this }} t)
{% endif %}

ORDER BY o.order_updated_at_date_time DESC