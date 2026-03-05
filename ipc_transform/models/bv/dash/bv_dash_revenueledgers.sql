{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_revenueledgers as (
SELECT 
  id_hash_key              AS revenue_ledgers_id_hash_key,
  "_id"                   AS revenue_ledgers_id_pk,
  amount                  AS revenue_ledgers_amount,
  description             AS revenue_ledgers_description,
  "revenueAccount"        AS revenue_ledgers_revenue_account,
  "type"                  AS revenue_ledgers_type,
  "createdAt"             AS revenue_ledgers_created_at_date_time,
  "updatedAt"             AS revenue_ledgers_updated_at_date_time,
  "__v"                   AS revenue_ledgers_version,
  reference               AS revenue_ledgers_reference,
  record_load_date        AS revenue_ledgers_record_load_date
  from {{ ref('raw_dash_revenueledgers') }}

)

select *
from bv_revenueledgers 



