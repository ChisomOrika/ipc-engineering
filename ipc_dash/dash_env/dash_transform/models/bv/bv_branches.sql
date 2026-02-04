{{ config(materialized='table',schema = 'bv',tags=['Python_Gosource']) }}

with raw_branches as (
    select * 
    from {{ ref('raw_branches') }}  -- Reference the raw data in staging
),


bv_branches as (select 
  _id as branches_id_pk,
  name as branches_name,
  address as branches_address,
  state as branches_state,
  lga as branches_lga,
  lat as branches_latitude,
  lng as branches_longitude,
  "longestDistance" as branches_longest_distance,  
  "customerId" as branches_customer_id_fk,        
  "isHeadquarters" as branches_is_headquarters,
  "createdAt" as branches_created_at_date_time,
  "updatedAt" as branches_updated_at_date_time,
  __v as branches_v,
  "isActive" as branches_is_active,
  "chowdeckMerchantApiKey" as branches_chowdeck_merchant_api_key,
  "chowdeckMerchantName" as branches_chowdeck_merchant_name,
  "chowdeckMerchantReference" as branches_chowdeck_merchant_reference,
  private as branches_private
from raw_branches)


SELECT *
from bv_branches