{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_wallets as (
  SELECT
  id_hash_key         AS wallet_id_hash_key,
  "_id"               AS wallet_id_pk,
  reference           AS wallet_reference,
  balance             AS wallet_balance_amount,
  customer            AS wallet_customer_id_fk,
  account             AS wallet_account,
  branch              AS wallet_branch_id_fk,
  active              AS wallet_is_active,
  "createdAt"         AS wallet_created_at_date_time,
  "updatedAt"         AS wallet_updated_at_date_time,
  "__v"               AS wallet_version,
  "currentBalance"    AS wallet_current_balance_amount,
  "tempBalance"       AS wallet_temp_balance_amount,
  record_load_date    AS wallet_record_load_date
from {{ ref('raw_wallets') }}

)

select *
from bv_wallets















