{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_categories as (
    select * 
    from {{ ref('raw_categories') }}  -- Reference the raw data in staging
),

bv_categories as (SELECT 
       _id as categories_id_pk,
	   name as categories_name
               from raw_categories r )

SELECT *
from bv_categories