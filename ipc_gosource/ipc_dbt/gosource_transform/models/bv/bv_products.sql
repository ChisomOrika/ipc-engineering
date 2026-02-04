{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_products as (
    select * 
    from {{ ref('raw_products') }}  -- Reference the raw data in staging
),

bv_products as (SELECT 
       _id as product_id_pk,
	   REGEXP_REPLACE(name, '\(.*$', '') AS product_name, 
       discountprice as product_discountprice, 
       actualprice as product_actualprice,
	   brand as product_brand,
	   category as product_category_fk,
	   unit_key as product_unit_key,
	   unit_value as product_unit_value from raw_products r )

SELECT *
from bv_products
