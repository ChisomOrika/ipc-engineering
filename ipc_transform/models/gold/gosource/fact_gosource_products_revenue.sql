{{ config(
    materialized='incremental',
    schema='gold',
    tags=['GoSource'],
    unique_key='order_id'
) }}

WITH order_details AS (
    SELECT
        o.order_id_pk                                                       AS order_id,
        COALESCE(o.order_product_id_fk, o.order_product_id_fk_2)           AS product_id_fk,
        o.order_unified_customer_id_fk                                      AS customer_id,
        o.order_created_at_date                                             AS orders_date,
        o.order_updated_at_date                                             AS orders_updated_date,
        EXTRACT(MONTH FROM o.order_created_at_date)                         AS month,
        EXTRACT(DAY   FROM o.order_created_at_date)                         AS day,
        EXTRACT(YEAR  FROM o.order_created_at_date)                         AS year,
        TO_CHAR(o.order_created_at_date, 'Day')                             AS weekday,
        o.order_total_price_amount                                          AS revenue,
        o.order_service_charge_amount                                       AS service_charge,
        o.order_delivery_fee_amount                                         AS delivery_fee,
        o.order_product_discount_price                                      AS discount_price,
        o.order_product_actual_price                                        AS actual_price,
        o.order_product_quantity                                            AS quantity,
        o.order_status                                                      AS status
    FROM {{ ref('bv_gosource_orders') }} o

    {% if is_incremental() %}
      WHERE o.order_updated_at_date > (SELECT MAX(orders_updated_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM order_details
