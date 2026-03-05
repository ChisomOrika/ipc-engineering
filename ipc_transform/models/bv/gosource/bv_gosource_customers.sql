{{ config(materialized='table',schema = 'bv',tags=['GoSource']) }}

with raw_customers as (
    select * 
    from {{ ref('raw_gosource_customers') }}  -- Reference the raw data in staging
),

bv_customers as (select _id as customer_id_pk,
       "businessName" as customer_business_name,
       "canBuyOnCredit" as customer_can_buy_on_credit,
	   ROUND(CAST("creditAccount" AS NUMERIC), 2) as customer_credit_account,
	   verified as customer_verified from raw_customers r )

SELECT *
from bv_customers




