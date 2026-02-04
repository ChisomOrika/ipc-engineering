{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH receipts_details AS (
    SELECT 
        o.receipts_id_pk,
        o.unified_customer_id_fk, 
        c.customer_businessname AS customer_name,
               o.receipts_deliveryfee, 
               o.receipts_coupon,
               ROUND(CAST(o.receipts_servicecharge AS numeric), 2) AS service_charge,
               o.receipts_totalprice, o.receipts_subtotal, o.receipts_status, 
               o.receipts_createdat_date::date as receipts_createdat_date, o.receipts_deliveredat_date,
               o.receipts_createdat_date as receipts_createdat_date_time

    FROM {{ ref('bv_receipts') }} o
    left JOIN {{ ref('bv_customers') }} c  -- Reference the bv_receipts model
        ON c.customer_id_pk = o.unified_customer_id_fk
        
)

SELECT * FROM receipts_details





