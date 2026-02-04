{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_orders as (
    select * 
    from {{ ref('raw_orders') }}  -- Reference the raw data in staging
),

bv_orders as ( SELECT  _id  as ID_PK,
       customerid as orders_customerid_fk, 
	   COALESCE(customerid, business) as unified_customer_id_fk,
	   paymentmethod as orders_paymentmethod, 
	   subtotal as orders_subtotal, 
	   totalquantity as orders_totalquantity, 
	   ROUND(CAST(deliveryfee AS NUMERIC), 2) as orders_deliveryfee, 
	   servicecharge as orders_servicecharge, 
	   ROUND(CAST(totalprice AS NUMERIC), 2) as orders_totalprice, 
	   status as orders_status, 
	   createdat::date as orders_createdat_date, 
	   updatedat::date as orders_updatedat_date,
	   deliveredat::date as orders_deliveredat_date,
       shippedat::date as orders_shippedat_date, 
	   productid as orders_productid_fk, 
	   "product._id" as orders_productid_fk_2, 
	    product as orders_productid_fk_3,
	   "product.name" as orders_productname, 
	   "product.discountprice" as orders_product_discountprice, 
	   "product.actualprice" as orders_product_actualprice,
	    quantity as orders_productquantity,
	   "product.brand" as orders_productbrand, 
	   "product.category" as orders_productcategory,
	   unit as orders_productunit
    from raw_orders r
)

SELECT *
from bv_orders