{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH order_details AS (
    SELECT 
        o.ID_PK AS order_id,
        REPLACE(COALESCE(orders_productid_fk, orders_productid_fk_2, orders_productid_fk_3), '""','') AS product_id_fk,
        o.unified_customer_id_fk AS customer_id,
	    o.orders_createdat_date::date as orders_date,
	    EXTRACT(MONTH FROM orders_createdat_date) AS month,
        EXTRACT(DAY FROM orders_createdat_date) AS day,
        EXTRACT(YEAR FROM orders_createdat_date) AS year,
        TO_CHAR(orders_createdat_date, 'Day') AS weekday,
        o.orders_totalprice AS revenue,
        o.orders_servicecharge AS service_charge,
	    o.orders_deliveryfee AS deliveryfee,
	    o.orders_product_discountprice as discountprice, 
	    o.orders_product_actualprice as actualprice,
	    o.orders_productquantity as quantity,
        REPLACE(o.orders_status, '""', '') as status
    FROM {{ ref('bv_orders') }} o
)

SELECT * FROM order_details