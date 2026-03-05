{{ config(
    materialized='incremental',
    schema='gold',
    tags=['GoSource'],
    unique_key='receipts_id_pk'
) }}

WITH receipts_details AS (
    SELECT
        o.receipts_id_pk,
        o.unified_customer_id_fk,
        c.customer_business_name                                    AS customer_name,
        o.receipts_delivery_fee_amount,
        o.receipts_coupon,
        ROUND(CAST(o.receipts_service_charge_amount AS numeric), 2) AS service_charge,
        o.receipts_total_price_amount,
        o.receipts_subtotal_amount,
        o.receipts_status,
        o.receipts_created_at_date::date                            AS receipts_created_at_date,
        o.receipts_delivered_at_date,
        o.receipts_updated_at_date,
        o.receipts_created_at_date                                  AS receipts_created_at_date_time

    FROM {{ ref('bv_gosource_receipts') }} o
    LEFT JOIN {{ ref('bv_gosource_customers') }} c
        ON c.customer_id_pk = o.unified_customer_id_fk

    {% if is_incremental() %}
      WHERE o.receipts_updated_at_date > (SELECT MAX(receipts_updated_at_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM receipts_details
