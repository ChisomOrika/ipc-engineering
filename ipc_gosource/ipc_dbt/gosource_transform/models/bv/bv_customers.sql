{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_customers as (
    select * 
    from {{ ref('raw_customers') }}  -- Reference the raw data in staging
),

bv_customers as (select _id as customer_id_pk,   
       businessname as customer_businessname, 
       canbuyoncredit as customer_canbuyoncredit, 
	   ROUND(CAST(creditaccount AS NUMERIC), 2) as customer_creditaccount, 
	   verified as customer_verfied from raw_customers r )

SELECT *
from bv_customers




