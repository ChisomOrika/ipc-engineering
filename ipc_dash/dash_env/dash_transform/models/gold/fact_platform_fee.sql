{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}

with platform_fees as (
SELECT 
  revenue_ledgers_created_at_date_time::date as revenue_ledgers_created_at,
  sum(revenue_ledgers_amount) as revenue_ledgers_amount
  from {{ ref('bv_revenueledgers') }}
  GROUP BY
  revenue_ledgers_created_at

)

select *
from platform_fees