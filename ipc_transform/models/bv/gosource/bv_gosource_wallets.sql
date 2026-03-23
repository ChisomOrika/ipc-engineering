{{ config(materialized='incremental', schema='bv', tags=['GoSource'], unique_key='wallet_id_pk') }}

with bv_wallets as (
  select
    "_id"            as wallet_id_pk,
    "customer"       as wallet_customer_id_fk,
    "accountName"    as wallet_account_name,
    "accountNumber"  as wallet_account_number,
    "bankName"       as wallet_bank_name,
    "balance"        as wallet_balance,
    "active"         as wallet_active,
    "createdAt"      as wallet_created_at,
    "updatedAt"      as wallet_updated_at
  from {{ ref('raw_gosource_wallets') }}
  {% if is_incremental() %}
    WHERE "updatedAt" > (SELECT MAX(wallet_updated_at) FROM {{ this }})
  {% endif %}
)

select * from bv_wallets