{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_receipts as (
    select * 
    from {{ ref('raw_receipts') }}  -- Reference the raw data in staging
),

bv_receipts as (SELECT 
       _id as receipts_id_pk,
       customerid as receipts_customerid_fk,
	   COALESCE(customerid, business) as unified_customer_id_fk,
	   paymentmethod as receipts_paymentmethod,
	   subtotal as receipts_subtotal,
	   quantity as receipts_totalquantity,
	   ROUND(CAST(deliveryfee AS NUMERIC), 2) as receipts_deliveryfee, 
	   servicecharge as receipts_servicecharge, 
	   ROUND(CAST(totalprice AS NUMERIC), 2) as receipts_totalprice,
	   status as receipts_status, 
	   coupon as receipts_coupon,
	   createdat as receipts_createdat_date, 
	   updatedat::date as receipts_updatedat_date,
	   deliveredat::date as receipts_deliveredat_date,
       shippedat::date as receipts_shippedat_date, 
	   cancelledat::date as receipts_cancelledat_date from raw_receipts r )

SELECT *
from bv_receipts













