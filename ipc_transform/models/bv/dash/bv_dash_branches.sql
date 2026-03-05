{{ config(materialized='table',schema = 'bv',tags=['DAASH']) }}

with raw_branches as (
    select * 
    from {{ ref('raw_dash_branches') }}  -- Reference the raw data in staging
),


bv_branches as (select 
  _id as branch_id_pk,
  name as branch_name,
  address as branch_address,
  state as branch_state,
  lga as branch_lga,
  lat as branch_latitude,
  lng as branch_longitude,
  "longestDistance" as branch_longest_distance,  
  "customerId" as branch_customer_id_fk,        
  "isHeadquarters" as branch_is_headquarters,
  "createdAt" as branch_created_at_date_time,
  "updatedAt" as branch_updated_at_date_time,
  __v as branch_v,
  "isActive" as branch_is_active,
  "chowdeckMerchantApiKey" as branch_chowdeck_merchant_api_key,
  "chowdeckMerchantName" as branch_chowdeck_merchant_name,
  "chowdeckMerchantReference" as branch_chowdeck_merchant_reference,
  private as branch_private
from raw_branches)


SELECT *
from bv_branches